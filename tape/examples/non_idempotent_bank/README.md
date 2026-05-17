# Non-idempotent bank — the outbox + reconciliation contract, end to end

A worked example for the GCP hardening plan: an agent that wires money via a
bank API that **has no idempotency key support** — and yet, even with crashes,
the bank ends up with exactly one wire per logical operation.

## The pieces

| File             | What it is                                                          |
| ---------------- | ------------------------------------------------------------------- |
| `bank.py`        | A file-backed fake bank. `wire(...)` accepts no key; every call lands. |
| `connectors.py`  | `BankConnector` (`@tape.connectors.register`-ed). The **only** place that calls the bank. |
| `agent.py`       | The ADK agent. `wire_money` is `@tape.outbox_tool(...)` — body is intent-only. |
| `run.py`         | The demo driver: agent run → outbox dispatcher → reconciler. |

## The safety contract

```
agent tool body  →  records INTENT (BeginEffect, semantics=NON_IDEMPOTENT,
                                     dispatch=OUTBOX, business_key=...)
                  →  returns "accepted" to ADK (the body NEVER calls the bank)

outbox reactor   →  claim_effect_dispatch (atomic CAS lease)
                  →  BankConnector.dispatch(...)  → calls the bank ONCE
                  →  result:
                        confirmed → complete_effect(CONFIRMED)
                        failed    → record_dispatch_attempt(next_at=backoff)
                        unknown   → record_dispatch_attempt(next_at=0)
                                    → effect status = UNKNOWN
                                    → the reactor will NOT retry it

reconciler       →  for every UNKNOWN effect:
                       BankConnector.observe(effect) → asks the bank by business_key
                       → record_external_observation(resolution=...)
                       confirmed  → mark CONFIRMED
                       absent     → (NON_IDEMPOTENT) → mark FAILED  (do not re-issue)
                                    (IDEMPOTENT)     → re-open as PENDING
                       duplicate  → mark CONFIRMED + register compensation
                       stuck      → leave UNKNOWN for human triage
```

## Run it

```bash
# build server + install SDK
cd tape && just build

# start the server (one terminal)
./server/target/debug/tape-server --listen 127.0.0.1:7878 --store sqlite:/tmp/tape-nonidem.db

# run the demo (another terminal)
cd tape && TAPE_URL=tape://127.0.0.1:7878 TAPE_EXAMPLE_DIR=/tmp/tape-nonidem \
    python -m examples.non_idempotent_bank.run --reset

# the failure-mode variants:
python -m examples.non_idempotent_bank.run --reset --inject-unknown
python -m examples.non_idempotent_bank.run --reset --crash-after-wire
```

In all three runs the bank ledger ends up with **exactly one forward wire**.

## How the in-process demo maps to GCP

The demo runs everything in one process for clarity. Production looks like:

```
                                 ┌─────────────────┐
                                 │  Cloud Run      │
                                 │  agent service  │
                                 └────────┬────────┘
                                          │  gRPC (BeginEffect, …)
                                          ▼
                                 ┌─────────────────┐
                                 │  Cloud Run      │
                                 │  tape-server    │
                                 └────────┬────────┘
                                          │  AlloyDB Postgres wire
                                          ▼
                                 ┌─────────────────┐
                                 │   AlloyDB       │
                                 └────────┬────────┘
                                          │
                       ┌──────────────────┼──────────────────┐
                       ▼                  ▼                  ▼
              ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
              │ Cloud Run      │  │ Cloud Run      │  │ Cloud Run      │
              │ outbox reactor │  │ reconciler     │  │ recovery /     │
              │ + connectors   │  │ + connectors   │  │ timers /       │
              │                │  │                │  │ compensations  │
              └────────┬───────┘  └────────┬───────┘  └────────────────┘
                       │                   │
                       └─────────┬─────────┘
                                 ▼
                       ┌─────────────────┐
                       │  the bank API   │
                       │  (non-idempotent│
                       │   — that's why  │
                       │   the connector │
                       │   is the only   │
                       │   caller)       │
                       └─────────────────┘
```

The agent's tool body never moves; the connector is the place to swap fakes
for the real bank.
