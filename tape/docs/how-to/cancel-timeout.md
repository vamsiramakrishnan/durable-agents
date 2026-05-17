# Cancel & timeout patterns

You have three orthogonal knobs:

1. **Cancel a run** — `tape.cancel_run(run_id, reason=…)`. Cooperative;
   the agent bails at the next model/tool boundary.
2. **Time out a gate** — `tape.gate_tool(name, timeout_ms=…)`. The
   timer reactor fires `gate_timeout` and resumes the run with a signal.
3. **Extend a lease** — `tape.heartbeat(tool_context)`. For long-running
   tool bodies, so recovery doesn't decide the run is stale.

Use each for what it's designed for. Mixing them is where bugs come from.

## Cancel a run

```python
import tape

tape.cancel_run("r-...", reason="user-requested")
```

This marks the run `CANCELLED` in the journal. The next time the agent
checks (configurable; the default `TapePlugin(check_cancellation=True)`
checks at every model/tool boundary), it bails.

Tools that are mid-syscall **keep running** until they return — Tape
doesn't preempt. If you want cancellation to bite faster inside a tool
body, check explicitly:

```python
@tape.effect(compensate=...)
def slow_thing(tool_context, ...):
    for chunk in stream():
        if tape.is_cancelled(tool_context):
            raise tape.RunCancelled()       # journals a clean abort
        process(chunk)
```

`RunCancelled` is the abort signal the plugin recognises; raising it
short-circuits the rest of the tool call and lands the effect as
`FAILED(cancelled)`.

If the run has already committed an effect with a registered compensator,
cancellation does **not** automatically compensate it. You either:

- Walk obligations explicitly with `tape.compensate_run(run_id)` (saga
  semantics, LIFO).
- Or let the run move to `CANCELLED` and decide later whether to
  compensate.

This is intentional. Cancellation is your decision; compensation is a
separate one.

## Time out a gate

A gate is an `await`-shaped pause:

```python
@tape.gate_tool("approval", timeout_ms=15 * 60 * 1000)   # 15 minutes
async def approval_gate(tool_context):
    return await tape.await_signal(tool_context, "approval")
```

If the signal `approval` doesn't arrive in 15 minutes, the timer reactor
fires a `gate_timeout` and resumes the run with the special signal
`gate.timeout` (the gate's helper raises `tape.GateTimeout` on the agent
side; you decide how to react).

`gate_timeout` is one of the built-in timer kinds the timer reactor knows
about. The others are `redrive` and `reconcile`. You can also fire
your own timers:

```python
tape.set_timer(run_id="r-...", fire_at_ms=t_future, kind="periodic",
               payload={"shift": "morning"})
```

…and wire a handler that runs when the timer fires. Periodic timers
re-arm themselves by setting a fresh timer in the handler.

## Extend a lease

The recovery reactor decides a run is stale when its lease's TTL has
expired. The default TTL is comfortably long, but for tool bodies that
genuinely take minutes (e.g., a big batch wire), call `heartbeat`:

```python
@tape.effect(...)
def slow_batch_wire(tool_context, ...):
    for batch in chunks(...):
        tape.heartbeat(tool_context)        # extends the lease
        bank.wire_batch(batch, ...)
```

`heartbeat` is a no-op outside a tool body. It only meaningful while a
run is actively holding its lease.

## Common patterns

### Soft deadline, then cancel

```python
# Set a redrive timer at deadline; on fire, cancel the run.
deadline_ms = now_ms() + 10 * 60 * 1000
tape.set_timer(run_id, deadline_ms, kind="redrive", payload={"action": "deadline"})

# In your handler:
def on_redrive(run, payload):
    if payload.get("action") == "deadline":
        tape.cancel_run(run.id, reason="deadline-exceeded")
```

### Human-in-the-loop with escalation

```python
# Gate with a 1-hour timeout; on timeout, ping a backup approver.
gate = tape.gate_tool("primary_approver", timeout_ms=60 * 60_000)

async def step(tool_context, ...):
    try:
        return await gate(tool_context)
    except tape.GateTimeout:
        backup_gate = tape.gate_tool("backup_approver", timeout_ms=30 * 60_000)
        return await backup_gate(tool_context)
```

### Long batch with progress

```python
@tape.effect(...)
def bulk_upload(tool_context, items):
    for i, item in enumerate(items):
        tape.heartbeat(tool_context)        # don't get re-driven
        tape.set_value(ns=f"runs/{tape.run_id_of(tool_context)}",
                       key="progress", value={"done": i, "total": len(items)})
        upload(item)
```

The KV update is what a UI watches via `watch_value`. The heartbeat
keeps the lease fresh.

## Anti-patterns

- **Don't** use `cancel_run` to "abort and clean up." Cancellation
  marks the run cancelled; cleanup is `compensate_run` (LIFO over
  obligations). Different operations, different semantics.
- **Don't** call `heartbeat` from outside a tool body. It needs the
  `tool_context` to know which run's lease to extend.
- **Don't** set a `gate_timeout` shorter than your reactor's polling
  interval — the timer can fire late, which is fine, but you'll be
  surprised by the latency.

## See also

- [Reactors](../concepts/reactors.md) — the timer reactor in context.
- [Replay & resume](../concepts/replay.md) — what happens when a
  cancelled run gets re-driven before the cancellation is noticed.
- [CLI reference: `tape status`](../reference/cli/index.md#tape-status) —
  inspect cancelled / stuck runs.
