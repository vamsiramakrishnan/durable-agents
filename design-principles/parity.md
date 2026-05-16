# Tape vs. Temporal — a feature-parity audit

The honest comparison, organised by Temporal feature. Tape is the agent-runtime
ceiling (scoped to ADK), Temporal is a (much more mature) durable-execution floor
— they're not really rivals (the spec lists "Temporal/Restate engine behind
`tape.proto`" as v2). This page is for choosing between them today, and for
naming the gaps Tape closes (or doesn't yet).

## What Tape covers, end to end

| Temporal feature | Tape | Notes |
|---|---|---|
| Workflows (durable orchestration) | ✓ (your ADK agent) | Tape doesn't ask for workflow code — your agent stays as it is; `TapePlugin` journals decisions and effects through ADK's existing hooks |
| Activities (boundary-crossing units) | ✓ (every tool call) | `@tape.effect(compensate=…, status_check=…, retry=…)` declares them; `TapePlugin` journals input/output, status, errors |
| Activity retries with backoff | ✓ | `@tape.effect(retry=tape.RetryPolicy(max_attempts=N, initial_interval_s=…, backoff_coefficient=…, max_interval_s=…, jitter=…, retry_on=(…,), non_retryable=(…,)))` — same idempotency key passed to the counterparty on every attempt |
| Signals (named messages to a running run) | ✓ | `AwaitSignal` / `SendSignal`; `tape.gate_tool("approval")` is the `LongRunningFunctionTool` shape |
| Timers (`workflow.sleep`) | ✓ | `tape.set_timer(run_id, fire_at_ms, kind)` / `tape.cancel_timer`; the timer reactor (`tape.reactors.fire_due_timers_once`) fires them. Built-in kinds: `gate_timeout`, `redrive`, `reconcile`, plus your own via a callback |
| Cancellation | ✓ (cooperative) | `tape.cancel_run(run_id, reason=…)` marks the run CANCELLED; `TapePlugin(check_cancellation=True)` checks at the next model/tool boundary and bails (or `tape.is_cancelled(tool_context)` on demand). Not preemptive — a tool body that's mid-syscall keeps running until it returns |
| Compensation / Sagas | ✓ | `@tape.effect(compensate=…)` registers the inverse; `tape.compensate_run(run_id)` walks obligations LIFO; failures land in `stuck` (never silently "compensated") |
| Side effects (`workflow.side_effect`) | ✓ | `tape.sample(tool_context, fn)` calls `fn` once per run, journals the result, returns-from-history on re-drive; `tape.now()`, `tape.uuid()`, `tape.random()` are pre-wrapped |
| Heartbeats (extend an activity's deadline) | ✓ (run-level) | `tape.heartbeat(tool_context)` extends the run's lease — for long-running tool bodies, so the recovery reactor doesn't decide the run is stale and re-drive concurrently |
| Replay (re-derive workflow state from history) | ✓ | The whole point: the agent re-drives via ADK's `invocation_id`; `TapePlugin` short-circuits confirmed effects and replays recorded decisions; the *resume point* is the first seq the journal has no record of |
| Idempotency keys | ✓ (and named-by-decision) | The key is `run/decision-N/<tool>/<call_idx>` (or the ADK `function_call_id` derivative) — not a hash of inputs, which can be recomputed differently on replay |
| The third outcome (`unknown` ack) | ✓ | `EffectStatus.UNKNOWN` + the reconciler reactor that calls the registered `status_check`. Temporal doesn't have this as a first-class status — you build it on top |
| Budget as run state | ✓ | `tape.Budget(usd_cap=…, token_cap=…)`; `AdmitBudget` before, `ChargeBudget` after; spent counters survive crashes |
| Continue-as-new (truncate history; restart) | ✗ (planned) | Sketched in spec §6 / §13; not yet implemented. Workaround: end the run TERMINAL with a summary state, start a new session with that state as the seed |
| Schedules (cron / interval) | ⚙️ (use timers) | Pattern: `set_timer(fire_at_ms=next_cron_tick, kind="periodic", payload=…)`; the handler does the work and re-arms with `set_timer(fire_at_ms=next_after_that)`. A dedicated `tape_schedules` table + cron parsing is a v2 add |
| Child workflows | ✗ (planned) | Sketched — `parent_run_id` on `tape_runs` + cascading cancel + "wait for child" via signals. Workaround: spawn a fresh `begin_run` and signal back when done |
| Versioning / patching (`workflow.patched`) | ✓ (manual) | `tape.policy_is(tool_context, "cfo-2026.05")` reads the recorded `policy_version` — `TapePlugin` records it on every decision; agents can branch on it. No automatic "use the new code path only for new runs" mechanism — you write the branch |
| Queries (read-only call on a running workflow) | ⚙️ (read the journal) | The journal + the session state are the queryable surface — `tape.TapeClient.get_run / get_effect / get_session / list_obligations / subscribe_events`. No "register a named query handler on the workflow" mechanism. The cross-run WAL tail (`SubscribeEvents`) covers observability |
| Updates (synchronous-mutating RPC, with validation) | ✗ | Use signals (`SendSignal` is the closest — async, no return value beyond the eventual run output) |
| Workers / task queues | ⚙️ (different model) | Tape doesn't have a worker pool; the agent process *is* the worker. Recovery is the reactor pulling from `ListRunsToRecover`. Multiple agent replicas serialise per-run via the lease |
| Pluggable persistence backends | ✓ | `sqlite:…` / `postgres:…` / `alloydb:…` / `bigtable:…` chosen by URL at deploy time (`TAPE_STORE`). Adding a backend = implementing the `RunStore` trait |
| Horizontal scaling of the server | ✓ | The Rust server is stateless; run N replicas behind a load balancer; the lease + idempotent RPCs make a double-drive harmless |
| Push-based event consumption | ✓ (WAL tail) | `SubscribeEvents` streams cross-run journal entries (`ts, run_id, seq`-ordered) — wire `tape.reactors.run_event_fanout(url, sink=…)` to Pub/Sub / Kafka / a webhook. On Bigtable: "use change streams" |
| Multi-language SDKs | ⚠️ (Python wired) | TS / Go / Java are scaffolds — the protocol is the stable surface; the ADK adapter is the mechanical work to finish |
| Web UI / Cloud (Temporal Cloud) | ✗ | Tape has no UI (just `SubscribeRun` / `SubscribeEvents` as machine feeds), no managed offering. Temporal Cloud removes ops; Tape is yours to run |
| Search attributes (custom indexed metadata) | ✗ | Roadmap |
| Replay-testing tooling (re-execute history with new code) | ✗ | Roadmap (the journal + the session events have everything needed) |
| Determinism enforcement (sandbox detects diverging replay) | ✗ | Tape can't sandbox ADK Python code. P11 documents the contract ("your code must be deterministic, route non-determinism through `tape.sample` / tools"); Temporal *enforces* it. Real difference, with real footguns on the Tape side if ignored |

## Choosing between them

**Pick Tape** when the job is "make my ADK agent durable, with minimal change to
the agent, and I want the agent-shaped primitives" — decision ledger,
decision-keyed idempotency, the `unknown` state + reconciler, gates as durable
suspends, budget as run state, model-written compensation, journaled
non-determinism — without a workflow rewrite. You self-host the Rust server +
(SQLite/Postgres/AlloyDB/Bigtable); the agent stays as ADK code.

**Pick Temporal** when you need a battle-tested, multi-language, generally-useful
durable-execution platform — non-agent use cases included — with a managed
option (Temporal Cloud), determinism enforced by the SDK, mature versioning,
schedules, child workflows, search attributes, and a Web UI. The cost is
expressing your agent as a workflow + activities, which for ADK is a rewrite.

**Pick both** — by putting Temporal under Tape (v2 in the spec: a Temporal-backed
`RunStore`). The agent keeps Tape's API, the durable execution is Temporal's;
you get the agent ceiling on top of the production-grade floor, which is the
combination the treatise's architecture diagrams point at.

## What's deferred, and how to bridge it today

- **Continue-as-new** — end the run TERMINAL with a summary state in the
  session; start a new session seeded with it. Plan: a `ContinueAsNew` RPC + a
  `tape.continue_as_new(tool_context)` helper.
- **Child runs** — `begin_run` a sub-run, signal-back when done. Plan: a
  `parent_run_id` qualifier + cascading cancel.
- **Cron-style schedules** — periodic timer that re-arms itself. Plan: a
  `tape_schedules` table + cron parsing, with a schedules reactor.
- **Named queries** — read `get_run` / `get_session` / `subscribe_events`. Plan:
  a `RunQuery` RPC that routes to an agent-registered handler (only useful when
  the agent process is alive — limited utility for the always-on case).
- **Updates** — use signals + the next decision boundary for the response. No
  near-term plan.
- **Sandbox-enforced determinism** — Python can't be sandboxed safely; lint
  `tape.sample` usage; document P11 prominently.

## Test coverage of this parity work

- `tape/tests/test_features.py` — retry policies (succeeds-after-retries,
  gives-up-on-non-retryable, exhausts-max-attempts), cancellation (`cancel_run`
  → `CANCELLED`; cancelled runs are not recoverable), policy-version branch.
- `tape/tests/test_resume.py` — the original kill-and-resume (3 cases).
- `tape/tests/test_reactors.py` — the timer reactor + the reconciler reactor.
- `tape/tests/test_bigtable.py` — the same kill-and-resume against the
  Bigtable backend (emulator-bootstrapped).
- Rust: `cargo test` — the in-process store + the gRPC service.
