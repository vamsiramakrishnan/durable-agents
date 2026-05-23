# Example chaos scenarios

A scenario file is a Python module that exposes two things:

```python
SCENARIO = chaos.Scenario(...)   # the bundle of faults + invariants + seed
def body(client, session):       # what the agent does under those faults
    ...
```

Drive them with the CLI:

```bash
# pre-flight check
tape chaos doctor

# run one scenario once
tape chaos run tape/examples/chaos_scenarios/smoke.py

# re-run with the same seed twice and check determinism
tape chaos replay tape/examples/chaos_scenarios/lose_ack_on_wire.py
```

Each example below also exits non-zero on a failed invariant, so any of
them slot into CI as smoke gates.

## What's here

| File | What it exercises | Needs a registered connector? |
|---|---|---|
| [`smoke.py`](smoke.py) | No faults; baseline invariants pass against a clean server. | No |
| [`lose_ack_on_wire.py`](lose_ack_on_wire.py) | The `bank.wire` connector returns UNKNOWN once. The reconciler resolves. Asserts exactly-one wire. | Yes — `bank.wire` from `examples/non_idempotent_bank` |
| [`outbox_race.py`](outbox_race.py) | Two outbox dispatchers race for the same effect. The CAS lease must yield single-winner. | Yes — `bank.wire` |
| [`server_failpoint.py`](server_failpoint.py) | A server-side failpoint at `tape::begin_effect::post_db`. **Requires `tape-server` built with `--features chaos`.** | No (server-layer fault) |

## Running them against the non-idempotent bank

```bash
# 1. Start the server (release for failpoints):
cd tape/server && cargo build --features chaos --release
./target/release/tape-server --listen 127.0.0.1:7878 \
    --store sqlite::memory: &

# 2. Register the example bank connector by importing the example
#    module (or by running its outbox dispatcher, which imports it).
#    For chaos scenarios that don't drive an agent, you'll need to
#    register the connector yourself — see lose_ack_on_wire.py's body.

# 3. Run a scenario:
tape chaos run tape/examples/chaos_scenarios/lose_ack_on_wire.py
```

## Writing your own

Copy `smoke.py` and add faults / invariants. The full surface lives at
`tape.chaos.*`:

* **Server-layer faults**: `chaos.crash(point=...)`, `chaos.delay(point=..., ms=...)`,
  `chaos.error(point=..., status=...)`.
* **Connector-layer faults**: `chaos.lose_ack(connector=..., tool=...)`,
  `chaos.duplicate(...)`, `chaos.delay_connector(...)`.
* **Invariants**: `chaos.no_stuck_obligations`, `chaos.no_blind_non_idempotent_retry`,
  `chaos.no_budget_overrun`, `chaos.no_orphan_compensation`,
  `chaos.exactly_one(connector=..., tool=...)`.

`Scenario.strict_faults` defaults to True — a fault that can't be applied
(connector not registered, etc.) FAILS the scenario instead of silently
passing. Set `strict_faults=False` only when you intentionally have
optional faults.
