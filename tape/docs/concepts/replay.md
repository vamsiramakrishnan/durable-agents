# Replay & resume

When the agent re-attaches to a run — because it crashed, because the
pod got evicted, because the recovery reactor decided the lease was stale
— what does "resume" actually mean?

## The resume point

The **resume point** is the first `seq` in the journal that has no record.

Concretely:

- The agent (re-)constructs an ADK `Runner` via the runner factory.
- The `TapePlugin` re-attaches the same `invocation_id`. ADK's resume
  semantics kick in.
- The plugin replays journaled decisions for `seq < resume_point` — the
  agent doesn't re-ask the model for choices it already made.
- It replays confirmed effects' results from history — the tool body
  doesn't run again.
- At `seq == resume_point`, the agent does what it would have done if it
  had never crashed.

## What gets replayed

| Record at `seq < resume_point` | What replay does |
|---|---|
| Decision | Hand the recorded choice back to the model loop |
| Effect, `CONFIRMED` | Hand the recorded result back to the tool caller |
| Effect, `PENDING` | The reconciler is on it; agent waits |
| Effect, `UNKNOWN` | Same — wait for the reactor to resolve |
| Budget admit/charge | Restore spent counters |
| Gate, signalled | Hand the signal to the agent |
| Gate, awaiting | Suspend until signalled or timed out |

The replay is **passive** — it doesn't call any tool bodies, doesn't call
the model. It rebuilds the agent's view from history. The first thing
that *runs* is the work at `seq == resume_point`.

## Determinism is your job

For replay to be correct, your tool bodies and your agent's prompt
construction must be **deterministic given the journal**.

Three rules:

1. **No wall-clock reads outside `tape.sample`.** Wrap them:
   ```python
   now = tape.now(tool_context)                # journalled once per run
   uid = tape.uuid(tool_context)               # journalled once per run
   r   = tape.sample(tool_context, lambda: random.random())
   ```
2. **No reads from mutable external state outside an `@tape.effect`.**
   If the agent's prompt reads from a DB row, that read goes through
   `tape.sample` or is itself an effect.
3. **Same code path on re-drive.** If you bump the agent's model version
   between attempts, replay will diverge. Tape can't sandbox this, but
   it can warn you via `tape.policy_is(tool_context, "...")` so you
   branch correctly.

## What about model nondeterminism?

The model's *output* is the decision. The decision is journalled. On
replay we hand the recorded decision back to the model loop — we don't
ask the model again. So model temperature doesn't matter for replay.

The model's *input* — the prompt — needs to be the same shape on every
call. ADK gives you tools, message history, and tool results in a
deterministic order; if you build prompts yourself, route through
`tape.sample` for anything that isn't already in the history.

## Replay vs. rewind vs. fork

These are three different operations. Don't confuse them.

| Operation | What it does | Use it when |
|---|---|---|
| **Replay** | Reconstruct an in-flight run from `seq=0` to the resume point. | Recovery after crash. |
| **Rewind** | Truncate the journal at `seq=N`, replay to that point, and continue. | "Run that again, but make this choice differently." |
| **Fork** | Copy a run's journal up to `seq=N` into a new `run_id`, continue independently. | "What would have happened if we'd done X?" |

Replay is automatic. Rewind and fork are explicit operator actions and
should be rare — they're the closest thing Tape has to a `git rebase`.

## The redrive

`tape.redrive(run_id)` is "wake this run up, even though nothing changed
in the world." It's the operator's poke-with-a-stick. Useful when:

- A reactor wedged and you've fixed it.
- A connector was misconfigured at first dispatch; you've updated it and
  want the run to retry on the new config.
- You added a new signal handler in code and want existing waiting runs
  to re-evaluate.

Internally, `redrive` is just a timer of kind `redrive` that fires
immediately, which the recovery reactor picks up the normal way.

## When replay can't help you

- The journal is corrupted. (It shouldn't be; the server runs on a
  transactional store.)
- Your code diverges between attempts in a way Tape didn't sandbox.
  (Real footgun. Watch `policy_version` on every decision.)
- An effect that was `CONFIRMED` actually didn't commit and the upstream
  has since changed its mind. (This is a contract violation by the
  upstream. STUCK + human.)

In all three, the answer is the same: stop, look, decide. Tape doesn't
pretend it can recover from inconsistent state.

## Next

- [The journal](journal.md) — what gets recorded, structurally.
- [Reactors](reactors.md) — the recovery reactor that triggers
  replay.
- [Reactive shared state](reactive-state.md) — the *other* primitive
  for "things that change while the run is alive."
