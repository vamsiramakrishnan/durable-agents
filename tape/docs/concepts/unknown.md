# UNKNOWN — the third outcome

Distributed systems have three outcomes, not two:

1. **Success** — the request committed, you got the ack.
2. **Failure** — the request was rejected (4xx), you got the no.
3. **UNKNOWN** — *neither of the above*. The network dropped the ack, the
   server timed out, the proxy died mid-stream. Did the request commit or
   not? You don't know.

Most retry policies assume two outcomes. They treat UNKNOWN as "probably
failure" and retry. For idempotent upstreams this is fine. For
non-idempotent upstreams it **double-commits** — the wire goes out twice.

Tape makes UNKNOWN a **first-class status** on every effect.

## When UNKNOWN is set

The tool body's HTTP call (or gRPC, or queue publish, or whatever) returned
ambiguously:

- Network error before the response headers
- Timeout waiting for the response body
- 5xx with no body and no idempotent header echo
- Connection reset

The connector turns each of these into `DispatchResult(kind="unknown")`,
and the effect lands in `EFFECT_STATUS_UNKNOWN`. No retry happens.

## How UNKNOWN gets resolved

The **reconciler reactor** loops over `UNKNOWN` effects and asks the
connector — *out of band* — what really happened. The connector implements
an `observe(effect)` method that maps to whatever your upstream offers:

```python
def observe(self, effect):
    # Query the upstream's read API by our business key.
    resp = http.get(f"{self.observe_endpoint}?key={effect.business_key}")
    if resp.status == 404: return ObservationResult(kind="absent")
    if resp.status == 200:
        if resp.json()["count"] == 1: return ObservationResult(kind="present")
        if resp.json()["count"] > 1:  return ObservationResult(kind="duplicate")
    return ObservationResult(kind="inconclusive")
```

The reconciler translates the observation:

| `observe()` returns | Effect lands at | Reactor next step |
|---|---|---|
| `present` | `CONFIRMED` | Done |
| `absent` | `PENDING` | Outbox re-dispatches (the original never landed) |
| `duplicate` | `CONFIRMED` + obligation | Compensation reverses the extra |
| `inconclusive` | `STUCK` | Human gets paged |

## What you have to provide

For UNKNOWN to be resolvable, **at decoration time** you must give the
effect at least one of:

- `business_key` — a deterministic identity the upstream can search by
- `status_check` — a callable that does the same thing in code (for
  inline tools)
- `compensate` — a callable that *un-does* the act if it turns out it
  committed

If you provide **none** of these on a non-idempotent tool, the SDK refuses
to build the tool. There is no safe path forward.

## Why STUCK is good

When `observe()` returns `inconclusive` (the upstream is degraded, the API
is down, the answer is genuinely unknowable right now), the run moves to
`STUCK` and a human gets paged.

This is the point. Tape does not silently "decide" what happened. STUCK
says: *the runtime doesn't know, and it won't guess.* You — or your
on-call — resolve it by reading the upstream's books and either
acknowledging the commit (`tape resolve --confirm`) or compensating it
(`tape resolve --compensate`).

## A real example

Consider a wire transfer:

```python
@tape.outbox_tool(
    connector="bank.wire",
    business_key=lambda account, amount, date, **_: f"{account}:{amount}:{date}",
    status_check=find_wire,
    compensate=reverse_wire,
)
def wire_money(account, amount, beneficiary, date):
    return {"account": account, "amount": amount, "beneficiary": beneficiary, "date": date}
```

- **Network drops mid-POST** → effect is `UNKNOWN`.
- Reconciler calls `bank.wire`'s connector `observe()` with the business
  key.
- **Bank says "no such wire"** → effect re-dispatches. One wire goes out.
- **Bank says "one wire on file"** → effect is `CONFIRMED`. We're good.
- **Bank says "two wires on file"** → one was a duplicate.
  Compensation enqueues a reversal for the extra.
- **Bank is down / says "I don't know"** → STUCK. Human pages.

That's four outcomes for one logical wire, all distinguished, none
silently wrong.

## Pre-Tape, this would have been

```python
try:
    bank.wire(...)
except (Timeout, ConnectionError):
    # Retry? Don't retry? Have we wired? Roll a die.
    raise
```

That's the bug.

## Next

- [Compensation & sagas](compensation.md) — what to do when the act
  committed and you wanted it not to.
- [Reactors](reactors.md) — the reconciler reactor, in detail.
- [Non-idempotent upstreams](../non-idempotent-upstreams.md) — the
  end-to-end how-to.
