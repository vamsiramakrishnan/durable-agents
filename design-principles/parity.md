# Tape vs. {Temporal · LangGraph durable execution · Pydantic AI + DBOS}

The honest comparisons, organised by feature. Tape is the agent-runtime ceiling
(scoped to ADK); Temporal is a (much more mature) durable-execution floor; the
others sit in the same agent layer Tape sits in, with different opinions about
where the runtime should live. They're not all rivals — the spec lists
"Temporal/Restate engine behind `tape.proto`" as v2 — but they're the live
choices for "make my agent durable" today.

- [§1 Tape vs Temporal](#1-tape-vs-temporal) — feature-parity audit
- [§2 Tape vs LangGraph durable execution](#2-tape-vs-langgraph-durable-execution) — same layer, different shape
- [§3 Tape vs Pydantic AI + DBOS](#3-tape-vs-pydantic-ai--dbos-dbosagent) — same conclusion, different framework

## 1. Tape vs Temporal

### What Tape covers, end to end

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
| Reactive shared state (coordinate through journaled state, not messages) | ✓ (Tape-original; treatise §IX ⑥) | `tape.set_value(ns, key, v, if_version=…)` writes monotonically-versioned values with optional CAS; `tape.get_value(ns, key)` reads; `tape.watch_value(ns, key, from_version=0)` streams `ValueEvent`s for the snapshot + every change with `(prev_version, prev_value_json)` attached — so a subscriber observes the *transition* (X: 70 → 90) rather than just the latest. `tape.delete_value` tombstones (watchers see one final event with `deleted=True`). Different primitive from signals (point-to-point, single-consumer) and the WAL tail (cross-run, journal-of-everything) — this is shared *state*, fan-out, by-key. Temporal's nearest analogue is workflow updates + queries; the agent equivalent there requires rolling your own |
| Push-based event consumption | ✓ (WAL tail) | `SubscribeEvents` streams cross-run journal entries (`ts, run_id, seq`-ordered); wire it through `tape.reactors.run_event_fanout(url, sink=…)` for an in-process consumer or `tape.reactors.run_outbox_relay(url, sink, cursor_path=…)` for an **exactly-once-effective publisher** (durable cursor + at-least-once delivery + consumer dedup on `(run_id, seq)`). Built-in sinks: `LogSink`, `WebhookSink`, `PubSubSink` (Google Cloud Pub/Sub, lazy import). On Bigtable: "use change streams" for cross-run tail |
| Multi-language SDKs | ✓ (Python full · TS / Go / Java wired-client + tests) | The Python SDK is the reference (ADK plugin + session service + reactors + sinks). TS / Go / Java each ship: a working `TapeClient` covering all RPCs (run lifecycle, decisions, effects with the dedup short-circuit, obligations, budget admit/charge, gates, timers, reconciliation, the WAL tail, sessions), `tape://` plaintext + `tapes://` TLS (Java accepts a Bearer token; TS/Go auto-attach a Google ID token from ADC), and a smoke test that round-trips the full lifecycle against a real `tape-server`. The per-language ADK adapter (`TapePlugin` / `TapeSessionService` for each language's ADK port) is mechanical work on top — the protocol is the stable surface |
| Web UI / Cloud (Temporal Cloud) | ✗ | Tape has no UI (just `SubscribeRun` / `SubscribeEvents` as machine feeds), no managed offering. Temporal Cloud removes ops; Tape is yours to run |
| Search attributes (custom indexed metadata) | ✗ | Roadmap |
| Replay-testing tooling (re-execute history with new code) | ✗ | Roadmap (the journal + the session events have everything needed) |
| Determinism enforcement (sandbox detects diverging replay) | ✗ | Tape can't sandbox ADK Python code. P11 documents the contract ("your code must be deterministic, route non-determinism through `tape.sample` / tools"); Temporal *enforces* it. Real difference, with real footguns on the Tape side if ignored |

### Choosing between Tape and Temporal

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

### What's deferred, and how to bridge it today

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

### Test coverage of this parity work

- `tape/tests/test_features.py` — retry policies (succeeds-after-retries,
  gives-up-on-non-retryable, exhausts-max-attempts), cancellation (`cancel_run`
  → `CANCELLED`; cancelled runs are not recoverable), policy-version branch.
- `tape/tests/test_resume.py` — the original kill-and-resume (3 cases).
- `tape/tests/test_reactors.py` — the timer reactor + the reconciler reactor.
- `tape/tests/test_bigtable.py` — the same kill-and-resume against the
  Bigtable backend (emulator-bootstrapped).
- `tape/tests/test_values.py` — reactive store: write/get/CAS/delete roundtrip,
  CAS version-conflict rejection, and the headline X-70-to-90 watcher seeing
  both the snapshot and the transition with the previous value attached.
- Rust: `cargo test` — the in-process store + the gRPC service.

---

## 2. Tape vs LangGraph durable execution

Source for LangGraph claims: <https://docs.langchain.com/oss/python/langgraph/durable-execution>.

LangGraph and Tape sit in the same layer: they make a *graph-shaped agent*
durable without asking the developer to rewrite the agent as a workflow. The
shape of the answer is different — LangGraph cuts the journal at node /
entrypoint boundaries and asks the user to wrap non-determinism in `@task`;
Tape cuts the journal at the **decision** and **effect** boundaries the
treatise's §IX names, and the user wraps `@tape.effect(...)`. Both then ride
on the counterparty's idempotency for the actual exactly-once-effective
guarantee at the wire.

| Question (the treatise's reactive-defence ⓵–⑦, applied to both) | LangGraph (durable execution) | Tape |
|---|---|---|
| **Is state durable?** | ✓ via `checkpointer=` on `compile()` (memory / SQLite / Postgres saver). Cut: at node boundaries (StateGraph) or entrypoint boundaries (Functional API). `durability="sync"\|"async"\|"exit"` chooses when the cut commits | ✓ via the Rust server + `RunStore` (SQLite/Postgres/AlloyDB/Bigtable). Cut: per **decision** (every model call) and per **effect** (every tool call's intent + outcome), each written in one txn with the ADK event |
| **Does the trigger fire exactly-once?** | Within a thread (`thread_id`), the node either ran-to-completion (its checkpoint is committed) or it didn't (it re-runs on resume). The dedup for an effect *inside* a node still rides on the counterparty | The decision-keyed idempotency key (`run/decision-N/<tool>`) names the *decision* the model made, not its inputs. A `confirmed` effect short-circuits on re-drive; a `pending` effect re-issues with the same key; the counterparty dedupes |
| **Is the handler itself durable?** | Node body re-runs from the top on resume. Inside a Functional API entrypoint, `@task`-wrapped sub-units cache their results — a completed task returns from history, an incomplete one re-runs | The agent re-drives via ADK's `invocation_id`; recorded decisions short-circuit at `before_model_callback`; confirmed effects short-circuit at `before_tool_callback`. The body never runs for already-confirmed work |
| **Where is the timer?** | Within a node: ordinary Python (not durable across crashes). HITL `interrupt()` *does* hold the run across deploys via the checkpointer | `tape.set_timer(run_id, fire_at_ms, kind)`; a timer reactor fires due timers across processes. Kinds: `gate_timeout`, `redrive`, `reconcile`, custom |
| **Is the condition still true when the handler runs?** | User discipline; no built-in atomic check-and-set | Optimistic versioning on the reactive store (`tape.set_value(ns, key, v, if_version=…)`) closes the TOCTOU race |
| **Where is the journal?** | Checkpoints are state-versioned snapshots of the graph state. Not a decision/effect/obligation ledger — that shape is user-built on top | Three explicit ledgers — decision, effect, obligation — interleaved by `(run_id, seq)`. The journal *is* the audit |
| **Is replay deterministic?** | Documented contract: wrap non-deterministic operations in `@task` (Functional API) or in nodes; otherwise replay drift. *Not* sandbox-enforced | Documented (P11): route non-determinism through `tape.sample` / tools. *Not* sandbox-enforced. Same footgun shape as LangGraph |
| **Human in the loop** | `interrupt(payload)` pauses the graph; `invoke(Command(resume=…), config)` resumes with the user's reply. Survives crashes via the checkpointer | `tape.gate_tool("approval")` returns `pending` (a `LongRunningFunctionTool`); `SendSignal` resolves it; the recovery loop re-invokes the run; ADK injects the signal payload as the tool result |
| **Graceful drain** | `RunControl.request_drain()` stops after the current superstep and saves a resumable checkpoint; `invoke(None, config)` resumes | `tape.cancel_run(run_id, reason=…)` marks the run CANCELLED; the plugin bails at the next model/tool boundary. Cooperative, not preemptive |
| **The third outcome (`unknown` ack)** | ✗ — not first-class. You either rerun the node (re-issuing without a counterparty-side key is unsafe) or build your own reconciler | ✓ `EffectStatus.UNKNOWN` + a registered `status_check` reactor that asks the counterparty and flips `pending`/`unknown` → `confirmed` or re-issues with the same key |
| **Compensation / sagas** | User-built. The graph can have a compensating branch, but there's no obligation ledger that runs LIFO on failure | `@tape.effect(compensate=…)` registers the inverse at commit; `tape.compensate_run(run_id)` walks obligations LIFO; failures land in `stuck` (never silently "compensated") |
| **Budget as run state** | User-built (carry counters in graph state; the user enforces the cap in a node) | `tape.Budget(usd_cap=…, token_cap=…)`; `AdmitBudget` before, `ChargeBudget` after; spent counters survive crashes |
| **Multi-agent coordination through journaled state** | Subgraphs + shared state in the parent graph; no monotonically-versioned, fan-out-watchable shared store as a primitive | `tape.set_value` / `get_value` / `watch_value` — monotonically-versioned, CAS-able, watchers see the *transition* (X: 70 → 90) with the previous value attached |
| **Time travel / fork** | The checkpointer keeps every state version on the thread; you can `invoke(Command(...), config={"configurable": {"thread_id": t, "checkpoint_id": c}})` to resume from any past checkpoint (the linked `durable-execution` page does not document fork-from-checkpoint; the broader docs do). The durable-execution doc's focus is *resume*, not *time travel* | Not a feature. Tape replays forward from the resume point; the journal supports replay-testing but Tape is not a versioned state store you fork |
| **Language scope** | Python and TypeScript SDKs of LangGraph itself | Python full (the ADK reference); TS / Go / Java wired-client + tests. Wire protocol is gRPC, so the runtime survives the agent's choice of language |
| **Operational footprint** | In-process library + a checkpointer backend (your DB). No separate server | A separate Rust server (one process per cluster, behind a load balancer) + a backend (SQLite/Postgres/AlloyDB/Bigtable) |

**The honest summary.** LangGraph's durable execution and Tape are answering
adjacent questions with overlapping vocabulary. LangGraph asks *how do I make
this graph survive a crash?* and gives you a checkpointer, a `@task`
decorator, `interrupt()` for HITL, three durability modes, and a drain
primitive — all in-process, all bound to the graph's notion of "what is a
step". Tape asks *how do I make this ADK agent's **decisions**, **effects**,
and **obligations** survive a crash?* and gives you a separate journaling
server that the agent talks to over gRPC, with the third outcome (`unknown`),
obligation-ledger compensation, decision-keyed idempotency, and a reconciler
as first-class primitives — the §IX list, by name.

**Pick LangGraph's durable execution** when your agent is *already* a
LangGraph graph, your durability needs end at "resume the graph from the last
node boundary", and the in-process checkpointer model fits your operational
shape. The Functional API + `@task` + `interrupt()` is a real, mature
implementation of the "checkpointed graph" pattern.

**Pick Tape** when your agent is an ADK agent, you need the §IX primitives
(`unknown`, model-written compensation, decision-keyed idempotency, gates as
durable suspends, budget as run state, coordination through journaled state),
and you can run the Rust server alongside your existing DB. The contract is
"the agent stays as ADK code; the journal lives somewhere built to survive."

**The composition.** Putting LangGraph *on top* of Tape is out of scope (Tape
is wired to ADK's callbacks, not LangGraph's). Putting Tape *on top* of a
LangGraph checkpointer is the wrong shape (LangGraph already commits at node
boundaries; Tape would duplicate the cut). Where they meet honestly is the
landscape claim Section XII makes: pick one runtime layer per agent, and
prefer the one built for the boundaries you actually care about.

---

## 3. Tape vs Pydantic AI + DBOS (DBOSAgent)

Source for Pydantic AI + DBOS claims: <https://pydantic.dev/articles/pydantic-ai-dbos>.

Pydantic AI's `DBOSAgent` is the closest spiritual cousin Tape has. Both
inherit the §XII conclusion — *put a durable engine underneath* — and apply
it to a single agent framework. The differences are framework (Pydantic AI vs
ADK), engine (DBOS in-process Postgres library vs Tape's stand-alone Rust
server), and which §IX primitives are first-class.

| Concern | Pydantic AI + DBOS (`DBOSAgent`) | Tape (ADK) |
|---|---|---|
| **Integration shape** | `DBOSAgent(agent)` wraps `Agent.run()` / `Agent.run_sync()` as a `@DBOS.workflow` and model + MCP calls as DBOS steps. Two lines: `from pydantic_ai.durable_exec.dbos import DBOSAgent; dbos_agent = DBOSAgent(agent)` | `Runner(..., plugins=[TapePlugin()], session_service=TapeSessionService(...))`. Two lines, no agent rewrite |
| **Durable engine** | DBOS — an in-process Python library backed by Postgres. No separate server; the DB is the control plane | Tape — a stand-alone Rust server (gRPC) + pluggable `RunStore` (SQLite/Postgres/AlloyDB/Bigtable). Separate process; the agent talks to it |
| **Workflow identity** | DBOS workflow UUID; identity flows from the framework | ADK's `invocation_id` (one `runner.run()` call) + `session_id`; Tape's `run_id` keyed to `(app_name, user_id, session_id, invocation_id)` |
| **Step identity (the recovery model)** | Step ID by **call order** inside the workflow — the model the treatise §VI calls out as the DBOS pattern | `seq` per run, monotonic by call order within `(run_id, kind)`. **Same model** (Tape acknowledges this in tape.md §6.5: "DBOS's step-id-by-call-order") |
| **Decision journal (the LLM call)** | Model calls are auto-wrapped as DBOS steps; the response is checkpointed and replayed from the DB on re-drive | `before_model_callback` short-circuits with the recorded `LlmResponse`; `after_model_callback` writes to `tape_decisions`. Same outcome (the decision is replayed, not re-sampled) |
| **Effect journal (the tool call)** | Tool invocations wrap as DBOS steps; results are checkpointed | `before_tool_callback` → `BeginEffect(pending)` *commits before the body runs*; `after_tool_callback` → `CompleteEffect(confirmed)`. The intent-before-act split is explicit |
| **Idempotency at the wire** | The user supplies the idempotency key inside the tool body (no decision-keyed key is mentioned in the article) | Tape *derives* the key — `run/decision-N/<tool>/<call_idx>` — and hands it back via the plugin; the body passes it to the counterparty |
| **The third outcome (`unknown` ack)** | Not documented as first-class. A failed step retries (DBOS retries failed steps); reconciliation against the counterparty is user-built | `EffectStatus.UNKNOWN` + `status_check` reactor — first-class |
| **Compensation / sagas** | DBOS has step-level retries; the article doesn't show a compensation primitive. Compensation is user-built as a separate workflow path | `@tape.effect(compensate=…)` registers the inverse at commit time; `tape.compensate_run` walks LIFO |
| **Human in the loop** | Not covered in the article. Pattern would be a DBOS workflow that awaits a signal/event | `tape.gate_tool("approval")` — `LongRunningFunctionTool` + signal; the run holds in `waiting` across deploys |
| **Sub-agent / fan-out** | "Sub-agent runs as child workflows" — `DBOS.start_workflow_async(...)` for fan-out/fan-in. End-to-end reliability across agents | Tape's child-runs are sketched (planned in §13). Workaround: spawn a fresh `begin_run` and signal back |
| **Durable queues** | Yes — DBOS includes Postgres-backed queues with concurrency limits, rate limits, retries, prioritisation | No queue primitive — the run lease + the reactor pattern serve the recovery case; cross-run fan-out is via the WAL tail + sinks (`PubSubSink`, `WebhookSink`) |
| **Budget as run state** | Not documented in the article | `tape.Budget` — admit before / charge after, survives crashes |
| **Coordination through journaled state** | Not documented as a primitive | `tape.set_value` / `watch_value` — monotonically-versioned, CAS, watchers observe the *transition* |
| **Observability** | DBOS Conductor (web UI), Pydantic Logfire via OpenTelemetry, MCP servers for natural-language queries | `SubscribeRun` / `SubscribeEvents` as machine feeds; no UI. The journal is the queryable surface |
| **Determinism** | Article doesn't address it explicitly. DBOS, like Tape, relies on user discipline to keep workflows replay-safe | P11: documented, not enforced. Same shape |
| **Operational footprint** | DBOS as a library + Postgres. No new infra | Rust server + the chosen backend. New infra |
| **Language scope** | Python (Pydantic AI's home) | Python (reference SDK) + TS / Go / Java wired-client. The wire protocol means the agent's language is independent of the runtime's |

**The honest summary.** Pydantic AI + DBOS and Tape are the *same conclusion*
applied to two different agent frameworks: the runtime layer is something
else, the agent stays as the framework's code, and the framework cedes
durability to a purpose-built engine. The two diverge on three real axes:

1. **Engine deployment.** DBOS is an in-process library on Postgres. Tape is
   a separate Rust server with multiple backends. DBOS is operationally
   simpler if you already run Postgres; Tape decouples the runtime from the
   agent's process and language at the cost of a new service.
2. **Which §IX primitives are first-class.** DBOS gives you durable
   workflows, steps, child workflows, queues, retries, and observability —
   the *floor* primitives. Tape adds the agent-shaped *ceiling*: `unknown`
   acks, decision-keyed idempotency, gates as suspend-until-signal,
   model-written compensation walked LIFO, budget as run state, and
   coordination through versioned shared state — the §IX list, by name.
3. **Framework scope.** DBOSAgent is Pydantic AI only. Tape is ADK only.
   Picking one is mostly picking the agent framework.

**Pick Pydantic AI + DBOS** when you want Pydantic AI's typed-agent ergonomics
and DBOS's operationally-simple Postgres-backed runtime, and the §IX *floor*
(workflows, steps, queues, retries) is enough — you'll build the ceiling
primitives (compensation walked LIFO, `unknown` reconciliation, decision-keyed
idempotency, budget as state) yourself when you need them.

**Pick Tape** when you're on ADK, you want the §IX *ceiling* primitives as
table stakes, and you can run the Rust server alongside your DB. The runtime
is independent of your agent's language; the journal is the audit; the
recovery model is the same step-by-call-order DBOS uses, expressed through
ADK's callbacks.

**The composition.** A `RunStore` backed by DBOS is *not* in the spec (Tape's
backends are SQLite/Postgres/AlloyDB/Bigtable — storage, not durable
execution). A Temporal-backed `RunStore` is (v2). The composition that makes
sense across all three is the §XII picture: one runtime layer per agent —
DBOS-under-Pydantic-AI, Tape-over-ADK, Temporal-under-Tape — chosen for the
boundaries the workload cares about.
