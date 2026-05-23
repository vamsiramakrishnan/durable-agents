# tape-adk

The ADK-aligned form of Tape's contract. Extends ADK's `DatabaseSessionService`
with an effect ledger, obligation ledger, and server-side timer registry —
maintained in the **same SQL transaction** as `append_event`. No separate
server, no separate database, no separate proto wire to manage.

```python
from tape_adk import TapeSessionService

session_service = TapeSessionService(db_url="sqlite+aiosqlite:///./tape.db")
# … pass to a `Runner` exactly like `DatabaseSessionService`.
```

For agents that call non-idempotent upstreams (wires, payments, irreversible
sends), pair it with the safety plugin:

```python
from tape_adk import NonIdempotentSafetyPlugin, outbox_tool

@outbox_tool(
    business_key=lambda account, amount, date: f"{account}:{amount}:{date}",
    connector="bank.wire",
    compensate="reverse_wire",
)
def wire(account: str, amount: int, date: str) -> dict:
    return bank.wire(account=account, amount=amount, date=date)

plugin = NonIdempotentSafetyPlugin(session_service=session_service)
runner = Runner(app=App(..., plugins=[plugin]), session_service=session_service)
```

The plugin journals an intent before the call goes out, refuses to call
non-idempotent tools inline, and (paired with the outbox dispatcher + the
reconciler — both library functions you run as Cloud Run Jobs or in a
sidecar) provides the `UNKNOWN`-aware exactly-once-effective contract that
ADK's own `ResumabilityConfig` openly admits is at-least-once.

This package is the *default tier*. For higher write throughput than
ADK-on-Postgres can deliver — or for Bigtable / Spanner backends — see the
companion **`tape-server`** (Rust gRPC). Both implement the same logical
schema and invariants.
