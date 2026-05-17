# Your first crash-survival

Five minutes. We're going to crash an agent mid-tool-call and watch the
recovery reactor pick it up. The point isn't the demo — the point is that
you see Tape's **contract** with your own eyes:

> The confirmed effect is not re-executed. The pending effect is resolved
> (re-dispatched, observed, or compensated). The agent resumes from the
> first journal seq that doesn't have a record.

## Prereqs

- [Installed](install.md) the Python SDK + CLI
- Have a project directory; you don't need an agent yet

## Step 1 — scaffold a project

```bash
tape init treasury
cd treasury
pip install -e .
```

The scaffold ships with one agent (`app/agent.py`), one tool, and a
`tape.yaml`. Look at it. Everything is obvious.

## Step 2 — run the kill-resume demo

```bash
tape dev --kill-resume-demo
```

This brings up:

1. `tape-server` against `./.tape/dev.db` (SQLite)
2. The recovery reactor
3. The reconciler reactor
4. The outbox reactor
5. Your agent

Then it does three things:

1. Triggers the agent to start a run.
2. **Crashes** the agent process after `BeginEffect` but before
   `CompleteEffect`.
3. Watches the recovery reactor re-drive the run.

The expected output:

```text
✓ tape-server up
✓ reactors started (recovery, reconciler, outbox, timers, compensation)
✓ agent started
→ triggering run...
✗ agent killed mid-effect (pid 12345)
↻ recovery reactor: run r-... is RUNNABLE; re-driving
✓ agent re-attached at decision_index=2, seq=8
✓ effect `wire_money` short-circuited (already CONFIRMED)
✓ run terminated cleanly
ASSERT: upstream observed exactly 1 wire ✓
```

The last line is the contract. **One** wire, even though the agent
re-attached. That's Tape doing its job.

## Step 3 — look at what the journal recorded

```bash
tape status
```

You'll see one run, status `TERMINAL`, with `decisions=N`, `effects=M`. To
peek at the actual records:

```bash
tape doctor --dump-run r-<id>
```

You'll see the decision records, the effect records (with `CONFIRMED`),
the budget rows, and the lease history. **That's** the journal.

## Step 4 — go off-script

Try this:

```bash
# In one shell:
tape dev

# In another:
tape status --follow
# Then trigger your agent and watch the rows light up.
```

Kill the agent (`Ctrl-C`) at different points:

- Before any tool call → recovery reactor re-drives; the agent re-asks the
  model from the same decision index.
- After `BeginEffect`, before `CompleteEffect` → recovery reactor re-drives;
  the journalled effect is *replayed from history* on the second attempt
  (not re-executed).
- After `CompleteEffect`, before the next model call → recovery re-drives;
  the next model call starts where it left off.

Each of these is a separate state of the world. Tape distinguishes between
them with the per-decision idempotency key and the effect's status.

## What you just demonstrated

- **Decisions are durable.** The agent doesn't re-ask the model for a
  decision it already made.
- **Effects are idempotent on the key.** A confirmed effect is *replayed
  from history*, not re-executed.
- **The reactor owns recovery.** You didn't write retry code. The reactor
  did it for you, **once**, on a lease.

## Next

- The mental model behind all of this: [**Concepts**](../concepts/index.md).
- The same thing, but with a non-idempotent upstream and the outbox:
  [**Non-idempotent upstreams**](../non-idempotent-upstreams.md).
- Now deploy this to GCP: [**Cloud Run**](../gcp-cloud-run.md).
