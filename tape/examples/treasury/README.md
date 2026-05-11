# The treasury agent — Tape-backed

This is the treatise's running example (["When the Orchestrator Isn't Code"](../../../design-principles/agents-that-act-treatise.md),
§II) with Tape underneath. Compare:

**Before** (treatise, hand-rolled ADK) — `execute_sweep` is ~25 lines of ceremony
spread across a tool body, a `before_tool_callback`, and `tool_context.state`:
idempotency key, journal check, intent write, the act with a three-way outcome,
session-state write, journal write, compensation register.

**After** (this directory — [`agent.py`](agent.py)):

```python
@tape.effect(compensate=reverse_wire, status_check=bank.wire_status)
def execute_sweep(account_id, amount_minor, target_mmf, rationale, tool_context):
    key = tape.idempotency_key(tool_context)
    return {"wire_id": bank.wire(account_id, amount_minor, target_mmf, idempotency_key=key)}
```

The tool body is just the act. `TapePlugin` records every decision, journals
every effect (intent first, then outcome, keyed off the decision), admits and
charges the budget, registers the compensations; `TapeSessionService` mirrors the
session and commits the journal in the same transaction. On a crash and re-drive,
the recorded decisions are replayed and the confirmed effects short-circuited, so
the day's book closes **once**.

## Pieces

| file | what |
|---|---|
| [`agent.py`](agent.py) | the agent, its three tools, and a deterministic scripted "policy" model (`ScriptedLlm`) so it runs without an API key — swap in `"gemini-2.5-pro"` for the real thing |
| [`fake_bank.py`](fake_bank.py) | `bank` / `broker` / `gl` — file-backed dedup ledgers (so they survive a process restart, like a real counterparty's idempotency) with a `TAPE_CRASH_AFTER=<tool>` crash hook |
| [`run.py`](run.py) | wires up `Runner(app=App(..., plugins=[TapePlugin()], resumability_config=...), session_service=TapeSessionService(...))` and runs it |

## Run it

```bash
# start a server
( cd ../../server && cargo run -- --listen 127.0.0.1:7878 --db /tmp/tape.db ) &

# one clean run
PYTHONPATH=../../sdk/python TAPE_URL=tape://127.0.0.1:7878 python -m treasury.run --reset
#   ... [treasury_agent] call execute_sweep(...) ; result wire-0001 ; call post_gl(...) ; "Book closed..."

# crash mid-run, then resume — and see one wire, not two
PYTHONPATH=../../sdk/python TAPE_URL=tape://127.0.0.1:7878 TAPE_LEASE_MS=1500 \
  TAPE_EXAMPLE_DIR=/tmp/tl TAPE_CRASH_AFTER=execute_sweep python -m treasury.run --reset      # exits 137
sleep 2
PYTHONPATH=../../sdk/python TAPE_URL=tape://127.0.0.1:7878 TAPE_LEASE_MS=1500 \
  TAPE_EXAMPLE_DIR=/tmp/tl python -m treasury.run --recover
cat /tmp/tl/bank.json     # one wire
```

(`just demo` and `just demo-resume` in [`../../justfile`](../../justfile) do this
for you. The integration test in [`../../tests/test_resume.py`](../../tests/test_resume.py)
asserts it.)
