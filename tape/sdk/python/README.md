# tape-py

The Python SDK and ADK adapter for [Tape](../../../design-principles/tape.md) —
a durable-execution substrate for ADK agents.

```bash
pip install -e .              # from this directory
# or, with the dev extras (protoc plugins, pytest):
pip install -e '.[dev]'
```

Two lines wire it into a runner:

```python
from google.adk.runners import Runner
from google.adk.apps import App
from google.adk.apps.app import ResumabilityConfig
from tape.adk import TapePlugin, TapeSessionService
import tape

app = App(name="treasury", root_agent=agent,
          plugins=[TapePlugin(budget=tape.Budget(usd_cap=50, token_cap=2_000_000))],
          resumability_config=ResumabilityConfig(is_resumable=True))
runner = Runner(app=app, session_service=TapeSessionService("tape://localhost:7878"))
```

A tool body stays plain; the decorator is optional sugar for declaring an
inverse / a status check:

```python
@tape.effect(compensate=reverse_wire, status_check=bank.wire_status)
def execute_sweep(account_id, amount_minor, target_mmf, rationale, tool_context):
    key = tape.idempotency_key(tool_context)        # = run/decision-N/execute_sweep/0
    return {"wire_id": bank.wire(account_id, amount_minor, target_mmf, idempotency_key=key)}
```

After a crash, re-drive recoverable runs:

```python
import tape
tape.recover_once(runner=runner)        # finds RUNNABLE / stale-RUNNING / signalled-WAITING runs and re-invokes them
```

## Regenerating the gRPC stubs

The generated stubs in `tape/_gen/` are committed so you don't need `protoc`.
To regenerate from `../../proto/tape.proto`:

```bash
./regen_protos.sh
```

## Parity

`tape-py` is the **reference** SDK — every primitive lands here first, then
in TypeScript / Go / Java. See [`../../../SDK_PARITY.md`](../../../SDK_PARITY.md)
for the live scorecard. The cross-SDK parity harness
([`../../../tape/tests/parity/`](../../tests/parity/)) drives the same
outbox-dispatch scenario through all four SDKs and asserts identical journal
state on every PR.

## Contribute

`make sdk-test-python` runs this SDK's round-trip tests; `make sdk-parity`
runs the cross-SDK harness. New primitives go through `tape/proto/tape.proto`
→ server → here → other three SDKs (in that order). See
[`../../../CLAUDE.md`](../../../CLAUDE.md).
