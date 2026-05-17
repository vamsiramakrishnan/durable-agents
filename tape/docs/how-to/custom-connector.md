# Write a custom connector

A **connector** is a typed adapter to one upstream. The outbox reactor
invokes it under a CAS lease for non-idempotent effects. The reconciler
reactor calls it to resolve `UNKNOWN`. The compensation reactor calls it
to undo committed effects.

Three methods. That's the whole protocol.

```python
from tape.connectors.base import (
    EffectConnector, DispatchResult, ObservationResult, CompensationResult
)

class MyConnector:
    name = "my.upstream"

    def dispatch(self, effect) -> DispatchResult:
        """Forward path. Idempotency key is on effect.idempotency_key."""
        ...

    def observe(self, effect) -> ObservationResult:
        """Out-of-band lookup. Called when an effect is UNKNOWN."""
        ...

    def compensate(self, obligation) -> CompensationResult:
        """Undo. Called when a duplicate is detected or the run is rolled back."""
        ...
```

## Register it

At project import time — the scaffold's `app/connectors.py` shows the
pattern:

```python
from tape import connectors
from app.my_connector import MyConnector

connectors.register(MyConnector(...))
```

`@tape.outbox_tool(connector="my.upstream", ...)` matches by `name`.

## The three results

### `DispatchResult`

```python
DispatchResult(
    kind="confirmed" | "failed" | "unknown",
    external_ref="some-id-from-the-upstream",   # if confirmed
    error_kind="4xx" | "5xx" | "timeout" | "network",  # if failed/unknown
    message="...",
    raw=...,        # whatever you want to journal
)
```

- `confirmed` — the upstream accepted and acked.
- `failed` — terminal no (a 4xx, an explicit rejection). No retry.
- `unknown` — ambiguous (timeout, network reset, 5xx without a useful body).
  The reconciler will call `observe()`.

The classification is **yours**. The connector encodes domain knowledge
about what each upstream signal really means.

### `ObservationResult`

```python
ObservationResult(
    kind="present" | "absent" | "duplicate" | "inconclusive",
    external_ref="...",                  # if present/duplicate
    message="...",
    raw=...,
)
```

- `present` — exactly one record matches your business key. CONFIRMED.
- `absent` — no record. The original dispatch never landed. Re-dispatch.
- `duplicate` — more than one record. CONFIRMED + obligation to compensate.
- `inconclusive` — the upstream is degraded; we don't know. STUCK.

The reconciler trusts your classification.

### `CompensationResult`

```python
CompensationResult(
    kind="resolved" | "failed",
    message="...",
    raw=...,
)
```

- `resolved` — the inverse operation committed.
- `failed` — couldn't undo. Obligation moves to STUCK; human gets paged.

## Idempotency on every call

The compensation reactor can be re-driven (the same lease can land in two
workers in pathological cases). The reconciler can be called repeatedly.
Build all three methods to be idempotent on the per-effect or per-obligation
key:

```python
def dispatch(self, effect):
    return http.post(
        f"{self.endpoint}",
        headers={"Idempotency-Key": effect.idempotency_key,
                 "X-Tape-Run-Id": effect.run_id,
                 "X-Tape-Effect-Key": effect.effect_key},
        json=effect.payload,
    )

def compensate(self, obligation):
    return http.post(
        f"{self.compensate_endpoint}",
        headers={"Idempotency-Key": f"compensate:{obligation.id}"},
        json={"reverse_of": obligation.external_ref},
    )
```

If the upstream supports idempotency keys natively, you're done. If it
doesn't, your `observe()` is doing the heavy lifting — it has to map the
upstream's view back to your effect's identity.

## A minimal HTTP connector

```python
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from tape.connectors.base import (
    DispatchResult, ObservationResult, CompensationResult,
)

class TicketConnector:
    name = "ticket.create"

    def __init__(self, endpoint, observe_endpoint, compensate_endpoint):
        self.endpoint = endpoint
        self.observe_endpoint = observe_endpoint
        self.compensate_endpoint = compensate_endpoint

    def dispatch(self, effect):
        req = Request(
            self.endpoint,
            method="POST",
            data=json.dumps(effect.payload).encode(),
            headers={
                "Idempotency-Key": effect.idempotency_key,
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read())
                return DispatchResult(kind="confirmed",
                                       external_ref=body["ticket_id"], raw=body)
        except HTTPError as e:
            if 400 <= e.code < 500:
                return DispatchResult(kind="failed", error_kind="4xx",
                                      message=str(e))
            return DispatchResult(kind="unknown", error_kind="5xx",
                                  message=str(e))
        except (URLError, TimeoutError) as e:
            return DispatchResult(kind="unknown", error_kind="network",
                                  message=str(e))

    def observe(self, effect):
        req = Request(
            f"{self.observe_endpoint}?key={effect.business_key}",
            headers={"Accept": "application/json"},
        )
        try:
            with urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read())
                n = len(body.get("tickets", []))
                if n == 0: return ObservationResult(kind="absent")
                if n == 1: return ObservationResult(kind="present",
                                                     external_ref=body["tickets"][0]["id"])
                return ObservationResult(kind="duplicate", raw=body)
        except (HTTPError, URLError, TimeoutError) as e:
            return ObservationResult(kind="inconclusive", message=str(e))

    def compensate(self, obligation):
        req = Request(
            f"{self.compensate_endpoint}/{obligation.external_ref}",
            method="DELETE",
            headers={"Idempotency-Key": f"compensate:{obligation.id}"},
        )
        try:
            with urlopen(req, timeout=10) as resp:
                return CompensationResult(kind="resolved")
        except HTTPError as e:
            if e.code == 404:
                return CompensationResult(kind="resolved",
                                           message="already-deleted")
            return CompensationResult(kind="failed", message=str(e))
```

## Built-in connectors as templates

- [`HTTPConnector`](../reference/python/connectors.md) — the generic
  HTTP + `X-Tape-*` headers connector. Read its source — it's small.
- [`PubSubConnector`](../reference/python/connectors.md) — publish to a
  Pub/Sub topic + a subscriber on the other side that does the actual
  upstream call. Useful when the upstream is best reached by an internal
  worker.

Both live in `tape/sdk/python/src/tape/connectors/`.

## Build tags (Go) / optional dependencies (Python)

The `PubSubConnector` lazy-imports `google-cloud-pubsub` so the SDK
doesn't drag a Google Cloud client into every install. The Go SDK gates
Pub/Sub behind the `pubsub` build tag and Cloud Tasks behind `cloudtasks`.

If your connector has a heavyweight dependency, do the same: lazy-import
in the constructor, and document the install incantation.

## Testing a connector

The full lifecycle is:

```python
# in tests/
def test_dispatch_confirmed(monkeypatch):
    fake = FakeHTTP({"POST /tickets": ({"ticket_id": "t-1"}, 200)})
    monkeypatch.setattr("urllib.request.urlopen", fake)
    c = TicketConnector("http://x", "http://x/lookup", "http://x")
    r = c.dispatch(make_effect(idempotency_key="r/d-1/x/0"))
    assert r.kind == "confirmed"
    assert r.external_ref == "t-1"
```

Then a kill-resume test against `tape dev --kill-resume-demo` is the
end-to-end proof. The bank example in `tape/examples/non_idempotent_bank/`
is the template.

## See also

- [Connectors (reference)](../reference/python/connectors.md) — protocol
  + built-ins.
- [Non-idempotent upstreams (how-to)](../non-idempotent-upstreams.md) —
  the broader pattern this connector slots into.
- [UNKNOWN — the third outcome (concept)](../concepts/unknown.md) — why
  `observe()` exists.
