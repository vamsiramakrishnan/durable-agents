# Non-idempotent upstreams

Some upstreams **cannot** be made idempotent. Wires, certain payment rails,
calls to brittle vendors that re-process on every POST. Tape's design point is
that these are honest about it — and that means three things must be true
**before** the dispatch happens:

  1. The intent is **journaled**. The agent's tool body returns the intent;
     it does NOT perform IO.
  2. A **reactor** owns the dispatch. The reactor is the only thing that
     actually calls the upstream.
  3. The intent declares **how to resolve UNKNOWN** — at least one of:
     `business_key=`, `status_check=`, or `compensate=`.

`@tape.outbox_tool(...)` (sugar for `@tape.effect(dispatch="outbox", ...)`)
enforces this at decoration time. A non-idempotent outbox tool without any of
those three raises `ValueError` (override with `allow_unsafe=True` after explicit
review). The server also enforces the contract at `BeginEffect`-time so even an
older SDK can't slip through.

```python
@tape.outbox_tool(
    connector="bank.wire",
    business_key=lambda account, amount, date, **_: f"{account}:{amount}:{date}",
    status_check=find_wire,
    compensate=reverse_wire,
)
def wire_money(account, amount, beneficiary, date):
    return {"account": account, "amount": amount,
            "beneficiary": beneficiary, "date": date}
```

## The state machine

```
intent journaled  ──►  outbox reactor: dispatch
                                │
                ┌───────────────┼───────────────────────┐
                ▼               ▼                       ▼
            CONFIRMED        UNKNOWN                  FAILED
                                │                       │
                          observe() returns         no retry
                                │                  (4xx-class)
            ┌───────────────────┼───────────────────────┐
            ▼                   ▼                       ▼
        CONFIRMED            ABSENT                  DUPLICATE
        (we're good)         (re-dispatch)           ↓ compensate
                                                    obligation
                                                    enqueued
```

The outbox reactor never retries a non-idempotent dispatch blindly. After an
UNKNOWN, it calls `connector.observe(effect)`. `observe()` is what the
counterparty's API looks like through your lens — query by business key,
search by idempotency key, whatever maps to your upstream.

If `observe()` returns DUPLICATE (a second wire really did happen), the
reactor registers a **compensation obligation** and the compensation reactor
calls `connector.compensate(obligation)` to reverse it. If `compensate()` is
unavailable or fails, the run becomes STUCK and a human gets paged.

## The minimal example

[`tape/examples/non_idempotent_bank/`](../examples/non_idempotent_bank/) walks
through:

  * **Scenario A**: crash *before* dispatch. Outcome: re-dispatch, one wire.
  * **Scenario B**: crash *after* dispatch but *before* recording. Outcome:
    UNKNOWN → `connector.observe()` → CONFIRMED, no duplicate.
  * **Scenario C**: real duplicate (the upstream did process twice).
    Outcome: observe → DUPLICATE → compensation obligation → reverse.
  * **Scenario D**: counterparty inconclusive. Outcome: STUCK → human gate.

Read it. The kill-test is the proof.

## Connectors

The outbox reactor invokes a registered connector under a CAS lease. Built-ins:
`HTTPConnector` (POST + `X-Tape-*` headers) and `PubSubConnector` (publish to a
topic, paired with a downstream subscriber that talks to the upstream). Custom
connectors implement the `EffectConnector` protocol (`dispatch` / `observe` /
`compensate`) — see [`tape/sdk/python/tape/connectors/base.py`](../sdk/python/tape/connectors/base.py).

```python
from tape import connectors
from tape.connectors.http import HTTPConnector

connectors.register(HTTPConnector(
    name="bank.wire",
    endpoint="https://bank.example/wires",
    observe_endpoint="https://bank.example/wires/lookup",
    compensate_endpoint="https://bank.example/wires/reverse",
))
```
