# Tape — A Durable-Execution Substrate for ADK Agents

*Design specification. Companion to ["When the Orchestrator Isn't Code"](agents-that-act-treatise.md).*

> The treatise ends on a sentence: *"Python will write the agent. Something else
> will run it."* And on a smaller one, inside the ADK walk-through: *"It needs a
> tape underneath the model that the regulator can read."* This document is the
> design of that something-else, that tape. It is scoped to one agent framework —
> Google's **Agent Development Kit (ADK)** — and it is built as a *separate
> system*: a high-concurrency, low-latency server with a language-agnostic wire
> protocol, so that the agent can stay in Python (or Java, or Go, or TypeScript)
> while the runtime that survives the model is written in something that was
> built to survive.

---

## 0. The one-paragraph version

**Tape** is a durable-execution server plus a set of thin SDKs. You point an ADK
agent at it with two lines — a `plugins=[TapePlugin()]` on the `Runner` and a
`session_service=TapeSessionService(...)` — and from then on every model call is
**recorded**, every tool call is made **exactly-once-effective**, every
human-or-event gate becomes a **durable suspend-until-signal**, every budget is a
piece of **run state** the loop checks before each step, and every irreversible
action carries a **compensation handle** so a later failure can unwind it. When
the agent process dies — deploy, crash, OOM — you re-invoke the run by its ADK
`invocation_id`; ADK re-drives the agent; Tape **replays the recorded decisions
and short-circuits the confirmed effects** so the run *reconstructs itself* and
continues from where it stopped, instead of re-deciding and re-acting. Tape adds
nothing *to* ADK — it rides only on extension points ADK already exposes (the
plugin system, custom `SessionService`s, `LongRunningFunctionTool`, and
`invocation_id`-based resume). Nothing in the agent's code changes; the tool
bodies stay plain.

---

## 1. From the treatise to this spec

The treatise's running example is a treasury agent that, once a day, reads
balances, prices the market, sweeps excess cash to a money-market fund, places a
hedge, and posts the general ledger. It walks the example through four agent
frameworks. The ADK version (treatise §II, "#### ADK") looks like this — the
*before*:

```python
def execute_sweep(account_id, currency, amount_minor, target_mmf, run_date,
                  rationale, tool_context) -> dict:
    # ① idempotency key — invocation_id helps when inputs would otherwise repeat
    key = hashlib.sha256(
        f"{run_date}:{account_id}:{amount_minor}:{target_mmf}".encode()).hexdigest()
    with side_journal() as j:
        # ② check the journal we built alongside the session
        if j.exists(key, status="confirmed"):
            return {"wire_id": j.wire_id(key)}
        # ③ write intent
        j.write(key, status="pending", account_id=account_id, amount_minor=amount_minor,
                target=target_mmf, rationale=rationale)
    # ④ the act
    try:
        wire_id = bank.wire(account_id, amount_minor, target_mmf, idempotency_key=key)
    except UnknownAck:
        with side_journal() as j: j.write(key, status="unknown")
        raise
    except Exception:
        with side_journal() as j: j.write(key, status="failed")
        raise
    # ⑤ session.state — what the model will read next turn
    tool_context.state[f"sweep:{account_id}:{run_date}"] = wire_id
    # ⑥ our journal — what survives the model
    with side_journal() as j: j.write(key, status="confirmed", wire_id=wire_id)
    # ⑦ compensation handle
    with comp_journal() as c:
        c.register(key, kind="reverse_wire",
                   payload={"wire_id": wire_id, "account": account_id})
    return {"wire_id": wire_id}
```

Seven points of ceremony — idempotency key, journal check, intent write, the act
with a three-way outcome, session-state write, journal write, compensation
register — hand-rolled, spread across a tool body and a callback and
`session.state`, and the developer keeps all three in step. The treatise's
verdict: *"The session is durable. The session is the wrong shape for a ledger.
Two ledgers, one process, one team paying for the gap."*

The treatise then names the **six primitives of the ceiling** (treatise §IX) —
the things the framework layer should provide so the developer stops hand-rolling
them:

| # | Primitive (treatise §IX) | What it means |
|---|---|---|
| ① | The idempotency key names the **decision**, not its inputs | Journal the model's instruction once; key the effect off the journal entry, not off values the model recomputes on replay. |
| ② | The third outcome — `unknown` — and the loop that resolves it | A side effect can be neither confirmed nor failed; that's a first-class state, and a reconciler closes it by asking the counterparty. |
| ③ | The action gate is a **signal**, not a flag | "Wait for the CFO to approve" is a durable suspend-until-signal, not a polled boolean. |
| ④ | Budget is a **workflow object**, not a wrapper | Spend caps are run state the loop admits against before each step and charges after. |
| ⑤ | Adaptive compensation: the model writes the inverse | Each irreversible effect registers, at the time it commits, the handle that unwinds it; failure runs them LIFO. |
| ⑥ | Coordination through **journaled state**, not messages | Multi-agent hand-off is a written fact in a shared log, not an in-flight message. |

…and §XII names the conclusion: *put a durable engine underneath.* This
spec is the design of an engine that provides ①–⑤ for ADK as table stakes (⑥, the
multi-agent coordination *entity*, is a v2 RPC group — see §11), with the seventh
point of ceremony — provenance — captured automatically because Tape sits on the
model-call boundary where the request, the response, and the policy version all
pass through.

> **The "after."** With Tape, `execute_sweep` is just `bank.wire(...)`. The
> decorator `@tape.effect(compensate=reverse_wire)` declares the inverse; the
> plugin does everything else. See §8 for the side-by-side.

---

## 2. Principles

These are the load-bearing decisions. Each maps to a piece of the treatise.

**P1 — Three ledgers, one journal.** Tape keeps a **decision ledger** (every
model call: request, response, rationale, policy version), an **effect ledger**
(every tool call: an idempotency key, a status in `{pending, confirmed, failed,
unknown}`, the request, the response, the error, a compensation reference), and an
**obligation ledger** (compensation handles registered alongside confirmed
effects). The three together, ordered by `(run_id, seq)`, are *the journal* —
the audit spine the treatise's regulator reads (treatise §III, fig.
[`03_three_ledgers.svg`](03_three_ledgers.svg); §XIII.4, fig.
[`34_three_ledgers_applied.svg`](34_three_ledgers_applied.svg)).

**P2 — The idempotency key names the decision.** When the model says "wire
£2,000,000 to fund X," that instruction is decision `#N` in the run. The effect's
idempotency key is `run/decision-N/<tool-name>` — derived from the *journal
entry*, **not** from a hash of the arguments, because the arguments could be
recomputed differently on replay if a prior tool result differed (a late credit
lands, `read_balances` returns a higher figure, `amount` changes, a hash of
`amount` changes, the bank sees a new instruction — *two wires*). This is treatise
§IX ①, fig. [`07_idempotency_gap.svg`](07_idempotency_gap.svg).

**P3 — `unknown` is a state, not an exception.** A tool whose call to a
counterparty timed out *after* the request may or may not have left, but did not
get an ack, ends in `unknown` — recorded, run not failed. A **reconciler** —
calling a per-effect-type status check the SDK registered — resolves it to
`confirmed` (record the counterparty's result) or `absent` (re-issue with the
same key) before the run can reach a terminal state or be re-driven past that
point. Treatise §IX ②, fig. [`24_unknown_ack.svg`](24_unknown_ack.svg).

**P4 — Intent before effect (the outbox move).** Before a tool body runs, Tape
writes the effect row with `status=pending` **and commits it**. The body executes
only after the intent is durable. A crash between the intent write and the
outcome write therefore always leaves a `pending` row — never a silent gap — and
that row is what the reconciler and the re-drive key off.

**P5 — Gates are durable suspends.** A `tape.gated(...)` action is an ADK
`LongRunningFunctionTool`: it returns `pending`, ADK pauses the run, Tape records
`status=waiting` plus the gate name. The run is now a row in Postgres that says
"run X is waiting on gate G" — it survives any number of deploys. `SendSignal`
writes the resolution and flips the run to `runnable`; the recovery loop (or a
webhook) re-invokes the run, ADK's long-running-tool resume protocol injects the
signal payload as the tool's result via `append_event`, and the run continues.
Treatise §IX ③, fig. [`25_human_gate_timeline.svg`](25_human_gate_timeline.svg).

**P6 — Budget is run state.** Each run has a `tape_budget` row: `usd_cap`,
`token_cap`, `usd_spent`, `tokens_spent`. `before_model` / `before_tool` calls
`AdmitBudget` (refuse the step if it would exceed a cap); `after_*` calls
`ChargeBudget`, recorded in the *same transaction* as the decision or effect
record. On resume the spent counters are read back from Postgres — a run that
crashed at $40 of a $50 cap resumes at $40, not $0. Treatise §IX ④.

**P7 — Compensation is registered at commit time, run LIFO, and can get stuck.**
When an effect with a declared inverse confirms, its compensation handle is
written to `tape_obligations` in the same transaction. If a later step fails
hard, `Compensate` walks the obligations newest-first and runs each inverse; a
compensation that succeeds marks its obligation `compensated`; one that fails
marks it `stuck` and fires an alert. Tape never reports a false "compensated."
Treatise §IX ⑤, and the loop's coda in §X.

**P8 — Replay is memory, not a process image.** Tape never snapshots the agent
process. On resume it *reconstructs* the run: ADK re-reads the session (events +
`state` dict) from Tape, re-drives the agent, and at each model call Tape returns
the recorded `LlmResponse` and at each tool call for an already-confirmed effect
Tape returns the recorded result — so the re-drive walks the same trajectory it
walked before, deterministically, until it reaches the first step Tape has no
record of: the **resume point**. From there everything proceeds for real.
Treatise §VIII "How replay actually works", §X, figs.
[`23_replay_timeline.svg`](23_replay_timeline.svg) and
[`10_replay_semantics.svg`](10_replay_semantics.svg).

**P9 — Provenance on every event.** Because Tape sits on the model-call boundary,
it records — without the developer doing anything — the model id, the request, the
response, and the policy version in `state`. The `rationale` argument a tool body
declares (if any) is recorded with the effect. The treasury regulator gets the
*why*, not just the *what*. Treatise §IX, the seventh point.

**P10 — Plug into the floor.** Tape does not reinvent durable storage (Postgres),
counterparty idempotency (it *passes through* the bank's own idempotency key),
the session/event store (it *is* an ADK `SessionService`), or run resumption (it
rides ADK's `invocation_id`). Treatise §XV "Floor and Ceiling", fig.
[`04_floor_and_ceiling.svg`](04_floor_and_ceiling.svg).

**P11 — Determinism is the agent author's contract.** Tape can replay the model's
non-determinism (it journals it) but it cannot replay non-determinism in the
*agent's own code* — a tool body that branches on `random()` or wall-clock time
will drift on re-drive. This is unenforceable in Python/Java/Go/TS; it is
documented (treatise §X, "anything non-deterministic is a runtime call or an
activity result") and surfaced as a lint in the SDK where it can be.

---

## 3. Why "no changes to ADK" holds

Everything Tape needs is a door ADK already opened. The mapping:

| Tape primitive | ADK extension point it rides on |
|---|---|
| Record a decision; replay it on re-drive | `Plugin.before_model_callback` (return a recorded `LlmResponse` → ADK skips the model call) / `after_model_callback` (capture the `LlmResponse`) |
| Journal an effect; skip a confirmed one on re-drive | `Plugin.before_tool_callback` (return a recorded result `dict` → ADK skips the tool body) / `after_tool_callback` (capture the result) / `on_tool_error_callback` (capture `failed` / `unknown`) |
| Mirror the conversation (events + `state`) durably, transactionally with the journal | a custom `BaseSessionService` — `TapeSessionService` — whose `append_event` writes the ADK `Event`, applies `event.actions.state_delta`, **and** writes the corresponding `tape_*` rows in one DB transaction |
| Action gate = durable suspend-until-signal | `LongRunningFunctionTool` (returns `pending`, pauses the run) + `SessionService.append_event` to inject the resolution later |
| Budget admit/charge | `before_model` / `before_tool` (admit) + `after_model` / `after_tool` (charge) |
| Run identity & resumption | ADK's `invocation_id` (one `runner.run()` call) + `session_id` (durable); Tape's `run_id` keyed to `(app_name, user_id, session_id, invocation_id)`; resume = `runner.run(..., invocation_id=stored)` against the session `TapeSessionService.get_session` reconstructs |
| Provenance | what flows through `before/after_model_callback` (request, response, model id) + `tool_context.state` (policy version) |

Tape sits **beneath** ADK, through these doors. There are two integration modes:

- **Explicit (recommended)** — two lines:
  ```python
  runner = Runner(agent=agent, app_name="treasury",
                  session_service=TapeSessionService("tape://localhost:7878"),
                  plugins=[TapePlugin()])
  ```
- **Zero-touch** — `tape run -- python my_adk_app.py`. The launcher imports a
  shim that, at import time, wraps `Runner.__init__` to inject the plugin and
  session service. (This is a monkeypatch; the spec says so plainly. Use it for
  retrofitting an existing app you don't want to edit; use the explicit form for
  anything you own.)

**Known ADK gaps, documented.** (1) Plugin callbacks do not fire for sub-agents
invoked under `AgentTool` — effects inside such a sub-agent must be journaled via
that inner agent's own callbacks or a wrapper; Tape ships a `TapeAgentTool` that
does this. (2) ADK's resume is caller-initiated, not automatic — Tape's recovery
loop *is* the caller (a small process that scans non-terminal runs and re-invokes
them). (3) "Tools may run more than once on resume" — that is exactly what the
effect ledger is for; the `before_tool` short-circuit is the mechanism that makes
the second run a no-op.

---

## 4. Architecture

```
   ┌───────────────────────────────────────────────────────────────────────┐
   │  product code (the CFO's app, the SRE's runbook, the support desk)     │
   ├───────────────────────────────────────────────────────────────────────┤
   │  ADK agent  —  LlmAgent, FunctionTool, Runner   (Python / Java / Go /  │
   │                                                  TypeScript)          │
   ├───────────────────────────────────────────────────────────────────────┤
   │  Tape SDK  —  TapePlugin · TapeSessionService · @tape.effect ·         │
   │               tape.gated · tape.Budget · TapeClient (gRPC)            │
   ├───────────────────────────────────────────────────────────────────────┤
   │  Tape server  (Rust · Tokio · Tonic · sqlx)                           │
   │   ┌─────────┬──────────┬──────────┬────────────┬─────────┬─────────┐  │
   │   │  Run    │ Decision │  Effect  │ Obligation │  Gate / │ Budget  │  │
   │   │ registry│  ledger  │  ledger  │   ledger   │ Signal  │ acct.   │  │
   │   └─────────┴──────────┴──────────┴────────────┴─────────┴─────────┘  │
   │   ┌──────────────┬───────────────────┬──────────────────────────────┐ │
   │   │  Reconciler  │  Recovery loop    │  Storage trait (PG · SQLite) │ │
   │   └──────────────┴───────────────────┴──────────────────────────────┘ │
   ├───────────────────────────────────────────────────────────────────────┤
   │  Postgres   (the journal + the ADK session/event mirror)              │
   ├───────────────────────────────────────────────────────────────────────┤
   │  systems of record  —  the bank, the broker, the GL, the ticketing... │
   └───────────────────────────────────────────────────────────────────────┘
```

*(Mirror of the treatise's [`08_architecture_layers.svg`](08_architecture_layers.svg);
the Tape-specific version is [`tape_stack.svg`](tape_stack.svg).)*

**Components.**

- **Run registry** — `tape_runs`: one row per `(app_name, user_id, session_id,
  invocation_id)`, with a `status ∈ {runnable, running, waiting, terminal,
  failed, compensating, stuck}` and a monotonic `seq` cursor.
- **Decision ledger** — `tape_decisions`: `(run_id, seq, model, request,
  response, rationale, policy_version)`. `RecordDecision` appends; `GetDecision`
  returns decision `#seq` on re-drive.
- **Effect ledger** — `tape_effects`: `(run_id, seq, decision_seq, tool_name,
  idempotency_key UNIQUE, status, request, response, error, compensation_ref)`.
  `BeginEffect` writes `pending` and commits, returns the decision-derived key;
  `CompleteEffect` writes the outcome; `GetEffect` is the re-drive short-circuit.
- **Obligation ledger** — `tape_obligations`: `(run_id, effect_seq, kind,
  payload, status ∈ {pending, committed, compensated, stuck})`.
  `RegisterCompensation` appends; `Compensate` walks LIFO.
- **Gate / Signal manager** — `tape_signals`: `(run_id, name, payload,
  consumed)`. `AwaitSignal` parks the run (`status=waiting`); `SendSignal`
  records the payload and flips it to `runnable`.
- **Budget accounting** — `tape_budget`: caps + spent counters. `AdmitBudget` /
  `ChargeBudget`.
- **Reconciler** — a periodic task: for every effect in `pending`/`unknown`,
  invoke the registered status check; flip to `confirmed` or re-issue.
- **Recovery loop** — on startup and on a timer: scan `tape_runs` for rows in
  `{runnable, running}` whose lease is stale, or in `waiting` with a consumed
  signal, and re-invoke them via the SDK's resume entrypoint
  (`runner.run(..., invocation_id=...)`).
- **Storage trait** — Postgres for production; SQLite for `tape dev` and tests.
  Migrations via `sqlx migrate`.

The **server is Rust** because the runtime that survives the model should be
written in something built to survive — high-concurrency (one server fronts many
agent processes), low-latency (it's on the model-call and tool-call hot path),
crash-safe, with explicit memory. The SDKs are thin gRPC clients; the agent stays
in whatever language the team writes agents in. This is the treatise's §XI
argument made concrete: *"Python will write the agent. Something else will run
it."*

---

## 5. The contract — `tape.proto`

The wire protocol is gRPC. The full definition lives in
[`../tape/proto/tape.proto`](../tape/proto/tape.proto); the shape:

```protobuf
service Tape {
  // ── run lifecycle ───────────────────────────────────────────────────────
  rpc BeginRun        (BeginRunRequest)        returns (BeginRunResponse);
  rpc ResumeRun       (ResumeRunRequest)       returns (ResumeRunResponse);
  rpc EndRun          (EndRunRequest)          returns (EndRunResponse);
  rpc GetRun          (GetRunRequest)          returns (RunState);
  rpc SubscribeRun    (SubscribeRunRequest)    returns (stream JournalEntry);

  // ── decision ledger ─────────────────────────────────────────────────────
  rpc RecordDecision  (RecordDecisionRequest)  returns (RecordDecisionResponse);
  rpc GetDecision     (GetDecisionRequest)     returns (DecisionRecord);   // re-drive

  // ── effect ledger ───────────────────────────────────────────────────────
  rpc BeginEffect     (BeginEffectRequest)     returns (BeginEffectResponse); // writes pending, commits, returns key
  rpc CompleteEffect  (CompleteEffectRequest)  returns (CompleteEffectResponse);
  rpc GetEffect       (GetEffectRequest)       returns (EffectRecord);     // re-drive short-circuit
  rpc ReconcileEffect (ReconcileEffectRequest) returns (EffectRecord);

  // ── obligations / compensation ──────────────────────────────────────────
  rpc RegisterCompensation (RegisterCompensationRequest) returns (RegisterCompensationResponse);
  rpc Compensate           (CompensateRequest)           returns (CompensateResponse); // LIFO

  // ── budget ──────────────────────────────────────────────────────────────
  rpc AdmitBudget     (AdmitBudgetRequest)     returns (AdmitBudgetResponse); // refuse if over
  rpc ChargeBudget    (ChargeBudgetRequest)    returns (BudgetState);

  // ── gates / signals ─────────────────────────────────────────────────────
  rpc AwaitSignal     (AwaitSignalRequest)     returns (AwaitSignalResponse);  // parks the run
  rpc SendSignal      (SendSignalRequest)      returns (SendSignalResponse);   // resumes it

  // ── ADK SessionService shim ─────────────────────────────────────────────
  rpc CreateSession   (CreateSessionRequest)   returns (Session);
  rpc GetSession      (GetSessionRequest)      returns (Session);
  rpc ListSessions    (ListSessionsRequest)    returns (ListSessionsResponse);
  rpc DeleteSession   (DeleteSessionRequest)   returns (DeleteSessionResponse);
  rpc AppendEvent     (AppendEventRequest)     returns (AppendEventResponse); // event + state_delta + tape projections, ONE txn
}
```

Design notes:

- **Every mutating RPC is idempotent.** It carries `run_id` and a `seq` (or, for
  `BeginEffect`, the `decision_seq`); replaying it is a no-op that returns the
  recorded result. This is what makes the SDK and the recovery loop safe to retry
  freely.
- **`AppendEvent` is the seam that gives single-transaction atomicity.** Because
  `TapeSessionService` routes ADK's `append_event` through Tape, Tape can wrap
  "insert the `Event`" + "apply `state_delta`" + "write the `tape_decisions` /
  `tape_effects` / `tape_obligations` row this event corresponds to" + "update
  `tape_budget`" in one Postgres transaction. There is never a moment where the
  event stream says "tool returned X" and the effect ledger does not know.
- **`SubscribeRun`** streams the journal (decisions, effects, obligations, gate
  events, state deltas) in `seq` order — for dashboards and live audit.
- **Streaming is used on the hot paths** (`SubscribeRun`; batched
  `RecordDecision`/`CompleteEffect` where the SDK pipelines) to keep latency
  low.

---

## 6. Data model

Postgres (SQLite for dev/test via the storage trait). All tables are
append-only except `tape_runs`, `tape_budget`, `tape_effects.status`,
`tape_obligations.status`, and `tape_signals.consumed`, which are the explicitly
mutable cursors.

| Table | Columns (abridged) | Role |
|---|---|---|
| `tape_runs` | `run_id PK, app_name, user_id, session_id, invocation_id, status, seq_cursor, lease_owner, lease_expires_at, started_at, ended_at` | the run registry |
| `tape_events` | `run_id, seq, invocation_id, author, branch, content JSONB, actions JSONB, ts` | the ADK `Event` mirror |
| `tape_state` | `scope ('run'|'user'|'app'), scope_key, key, value JSONB` | the current `Session.state` |
| `tape_decisions` | `run_id, seq, model, request JSONB, response JSONB, rationale, policy_version` | the decision ledger |
| `tape_effects` | `run_id, seq, decision_seq, tool_name, idempotency_key UNIQUE, status, request JSONB, response JSONB, error JSONB, compensation_ref` | the effect ledger |
| `tape_obligations` | `run_id, effect_seq, kind, payload JSONB, status` | the obligation ledger |
| `tape_signals` | `run_id, name, payload JSONB, consumed, created_at` | gates |
| `tape_budget` | `run_id PK, usd_cap, token_cap, usd_spent, tokens_spent` | budget state |

The **journal** the regulator reads is the union `tape_events ∪ tape_decisions ∪
tape_effects ∪ tape_obligations`, ordered by `(run_id, seq)`. History compaction
(for long-lived runs) happens at decision boundaries — the equivalent of
"continue-as-new": fold the prefix into a snapshot row, keep the tail. (v2.)

### 6.5 Resumability — what state Tape captures, when, where, and how a run is reconstructed

This is the heart of the design. A run is **reconstructed, not restored** — Tape
never snapshots a process image; it journals enough that re-driving the agent
reaches the same place. Per run, Tape captures **six kinds of state**,
append-only, each written in **one Postgres transaction with the ADK event it
corresponds to** (because `TapeSessionService` routes `append_event` through
Tape):

1. **Conversation state** — ADK's `Session`: its `events` list and its `state`
   dict (including `user:` / `app:` prefixes). ADK already persists this via
   `SessionService.append_event` (atomically appends the `Event` and applies
   `event.actions.state_delta`); Tape routes it to `tape_events` / `tape_state`.
   On resume, `get_session` rebuilds the full history + current `state` from
   Postgres; ADK's own resume machinery (`runner.run(..., invocation_id=X)`)
   replays committed events for that invocation and restarts from the first
   incomplete agent.
2. **Decision state** — *every* model call. The risk: ADK re-calls the LLM on
   re-drive → a *different* decision → a different trajectory. Tape's answer
   (Temporal's "LLM-call-as-an-activity," via ADK's model callbacks):
   `before_model_callback` asks Tape "for this run, is decision `#seq` recorded?"
   — **yes →** return the recorded `LlmResponse` (ADK short-circuits, does not
   call the model, gets the *same* decision); **no →** let it proceed, then
   `after_model_callback` writes the `LlmResponse` to `tape_decisions` with a
   monotonic `seq`. Position is the **ordinal of the model call within the run**
   (DBOS's step-id-by-call-order), not a prompt hash — stable as long as the
   agent graph re-drives the same way (the P11 caveat).
3. **Effect state** — *every* tool call's **intent-before-act** and its
   **outcome**. `before_tool_callback` → `BeginEffect`: Tape writes a
   `tape_effects` row `status=pending` **and commits before returning**, and
   hands back the **idempotency key derived from the journaled decision**
   (`run/decision-N/<tool>`, *not* a hash of the args). If a `confirmed` effect
   already exists for that key → `before_tool` returns the recorded result dict,
   ADK short-circuits, the body never runs. If `pending` exists (a prior run
   started it, then crashed) → the body runs again *with the same idempotency key
   passed to the counterparty* so the counterparty dedupes — plug into the floor.
   `after_tool_callback` → `CompleteEffect(confirmed, result)` (+
   `RegisterCompensation` if a `compensate=` was declared); `on_tool_error_callback`
   → `CompleteEffect(failed)` — or `unknown` if the error signals a lost ack. A
   crash *between* `BeginEffect(pending)` and `CompleteEffect` is the only window
   where uncertainty lives; the reconciler closes it.
4. **Budget state** — `tape_budget`. `before_*` → `AdmitBudget` (refuse if over);
   `after_*` → `ChargeBudget`, recorded in the *same txn* as the decision/effect
   record. On resume the counters are read from Postgres — a run that crashed at
   $40 of a $50 cap resumes at $40.
5. **Obligation state** — `tape_obligations`: compensation handles registered
   alongside `confirmed` effects, in the same txn. On resume they are intact, so a
   later failure can `Compensate` LIFO over exactly the effects that committed.
6. **Gate state** — `tape_signals` + `tape_runs.status`. A `tape.gated` action is
   a `LongRunningFunctionTool` that returns `pending`; Tape records
   `status=waiting` + the gate name. `SendSignal` writes the payload + flips to
   `runnable`; the recovery loop (or a webhook) re-invokes the run; ADK's
   long-running-tool resume injects the payload as the tool's result via
   `append_event`; the run continues. The *state of a multi-day wait* is "a row
   in Postgres saying run X waits on gate G" — it survives any number of deploys.

**How re-drive finds its place — `seq` alignment.** Two durable anchors compose:
ADK's **`invocation_id`** (the handle: resume = `runner.run(...,
invocation_id=stored, ...)` against the session rebuilt from `tape_events`) and
Tape's per-run monotonic **`seq`** (decisions and effects each get one, by call
order). On re-drive, the *k*th `RecordDecision`/`BeginEffect` call matches
recorded `seq=k`'s outcome (replay the decision; short-circuit the confirmed
effect); the **first call for a `seq` Tape has no record of** is exactly the
point the prior run reached — the **resume point** — and from there everything
proceeds for real. This is DBOS's recovery model, expressed through ADK's
callbacks. (Diagram: [`tape_seq_alignment.svg`](tape_seq_alignment.svg).)

**The reconciler — resolving `unknown`.** Before re-driving a run with a
`pending`/`unknown` effect, and again before a run can reach a terminal state,
Tape's reconciler calls a per-effect-type **status check** registered by the SDK
(e.g. "ask the bank for the status of `idempotency_key=K`") and flips
`pending`/`unknown` → `confirmed` (record the counterparty's result) or `absent`
→ re-issue with the same key. Without a registered status check, a `pending`
effect re-runs idempotently against the counterparty's own dedup window — still
safe within that window (treatise §IX ④ — "idempotency windows are bounded" — is
the caveat).

**What is deliberately NOT captured** — process-local memory (Python/JS objects,
the model's context window as a runtime value): resume *reconstructs* it by
replaying recorded decisions + reading session state; it does not snapshot it.
Untracked external state (a file written outside a tool, a `git push` via a shell
tool): out of scope — effects must flow through Tape-wrapped tools to be
journaled, and irreversible ones should be `tape.gated`. Non-deterministic
agent/tool code (P11): cannot be snapshotted; documented, not enforced.

**When Tape is NOT the session backend** (a user keeps `DatabaseSessionService`):
Tape falls back to the outbox pattern — `TapePlugin` writes `tape_*` rows in
Tape's own DB and a relay reconciles against the session DB; the integrated mode
(Tape as `SessionService`) is the recommended one and the only one with single-txn
atomicity. Both are supported.

(Diagram: [`tape_state_capture.svg`](tape_state_capture.svg) — the six state
kinds, each written in one txn with its ADK event.)

---

## 7. API surface (per SDK)

### Python (the reference SDK — `tape-py`, imported as `tape`)

```python
import tape
from tape.adk import TapePlugin, TapeSessionService

# 1. the two-line wiring
runner = Runner(
    agent=agent, app_name="treasury",
    session_service=TapeSessionService("tape://localhost:7878"),
    plugins=[TapePlugin()],
)

# 2. a tool body stays plain; a decorator declares the inverse + (optionally) a status check
@tape.effect(compensate=reverse_wire, status_check=bank.wire_status)
def execute_sweep(account_id: str, amount_minor: int, target_mmf: str,
                  rationale: str, tool_context) -> dict:
    key = tape.idempotency_key(tool_context)          # = run/decision-N/execute_sweep
    return {"wire_id": bank.wire(account_id, amount_minor, target_mmf, idempotency_key=key)}

# 3. a gate is one call
async def request_cfo_approval(amount_minor: int, tool_context) -> dict:
    return await tape.gated("cfo-approval", risk="irreversible",
                            payload={"amount_minor": amount_minor}, tool_context=tool_context)

# 4. budget is run config
runner.run(..., run_config=tape.with_budget(usd_cap=50, token_cap=2_000_000))

# 5. signal a waiting run from anywhere (a webhook, a CLI, another service)
tape.send_signal(run_id, "cfo-approval", {"approved": True, "by": "cfo@acme.com"})

# 6. resume helper (the recovery loop uses this; you can too)
tape.resume(invocation_id)         # ≅ runner.run(..., invocation_id=invocation_id)

# 7. raw client when you need it
client = tape.TapeClient("tape://localhost:7878")
```

`TapePlugin` is a `BasePlugin`: its `before_model_callback` short-circuits a
recorded decision, `after_model_callback` records one (and charges tokens);
`before_tool_callback` short-circuits a confirmed effect and runs `BeginEffect`
otherwise (and admits budget), `after_tool_callback` runs `CompleteEffect` +
`RegisterCompensation`, `on_tool_error_callback` records `failed`/`unknown`.
`@tape.effect(...)` is optional sugar — without it, `TapePlugin` still journals
every tool call as an effect with the default decision-derived key; you add the
decorator when you want to declare a `compensate=` inverse or a `status_check=` or
a custom `key_from=`.

### TypeScript / Go / Java (`tape-ts`, `tape-go`, `tape-java`)

Each ships: (a) the **generated `TapeClient`** from `tape.proto` — fully working,
connects to the same server, round-trips every RPC; (b) an **ADK adapter
scaffold** (`TapePlugin` + `TapeSessionService` for that language's ADK port)
that mirrors the Python one, with the callback-wiring left as a clearly-marked
`TODO` for v1. The protocol is the contract; finishing the adapters is mechanical
and tracked per-language.

---

## 8. Worked example — the treasury agent, Tape-backed

The full example is in [`../tape/examples/treasury/`](../tape/examples/treasury/).
The shape of it, side by side with the treatise's "before":

**Before (treatise §II "#### ADK")** — `execute_sweep` is ~25 lines of
hand-rolled ceremony across a tool body, a `before_tool_callback`, and
`tool_context.state`.

**After (Tape):**

```python
# agent.py
import tape
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool

def reverse_wire(wire_id: str, account_id: str, **_) -> dict:
    return {"reversal_id": bank.reverse(wire_id)}

@tape.effect(compensate=reverse_wire, status_check=bank.wire_status)
def execute_sweep(account_id: str, amount_minor: int, target_mmf: str,
                  rationale: str, tool_context) -> dict:
    key = tape.idempotency_key(tool_context)
    return {"wire_id": bank.wire(account_id, amount_minor, target_mmf, idempotency_key=key)}

@tape.effect(compensate=reverse_hedge, status_check=broker.order_status)
def execute_hedge(notional_minor: int, instrument: str, rationale: str, tool_context) -> dict:
    key = tape.idempotency_key(tool_context)
    return {"order_id": broker.place(instrument, notional_minor, idempotency_key=key)}

@tape.effect()                       # GL post is its own inverse-free record
def post_gl(entries: list, rationale: str, tool_context) -> dict:
    key = tape.idempotency_key(tool_context)
    return {"batch_id": gl.post(entries, idempotency_key=key)}

agent = LlmAgent(
    model="gemini-2.5-pro",
    instruction="Apply the CFO policy in session.state to today's positions.",
    tools=[FunctionTool(execute_sweep), FunctionTool(execute_hedge), FunctionTool(post_gl)],
)
```

```python
# run.py
from google.adk.runners import Runner
from tape.adk import TapePlugin, TapeSessionService
import tape

runner = Runner(agent=agent, app_name="treasury",
                session_service=TapeSessionService("tape://localhost:7878"),
                plugins=[TapePlugin()])

for event in runner.run(user_id="cfo", session_id="2026-05-11",
                        new_message="Close the book for today.",
                        run_config=tape.with_budget(usd_cap=50, token_cap=2_000_000)):
    ...
```

The tool bodies are plain. The decorator names the inverse. The plugin records
every decision, journals every effect with a decision-derived key, admits and
charges the budget, registers the compensations, and — on a crash and re-invoke —
replays the decisions and short-circuits the confirmed effects so the day's book
closes **once**. The example's `bank` / `broker` / `gl` are injectable fakes with
"lose the ack" and "crash here" hooks so the kill-and-resume demo is
deterministic (see §10).

---

## 9. Scope & non-goals

**v1 (this spec).** One Tape server, Postgres-backed (SQLite for dev/test).
Python + ADK fully wired and tested end to end. TS/Go/Java SDKs: generated
clients + smoke tests + adapter scaffolds. Primitives ①–⑤ of the treatise's
ceiling. Recovery via ADK `invocation_id` re-drive, driven by Tape's recovery
loop. Provenance captured automatically.

**Out of scope (v2 or never).**

- **Multi-agent coordination as a journaled entity** (treatise §IX ⑥, the
  Restate-virtual-object shape) — a `TapeEntity` RPC group; v2. v1 coordinates
  the simple way: shared `tape_state` keys.
- **Alternative backends behind the same protocol** — a Temporal or Restate
  *engine* underneath `tape.proto`; v2.
- **Multi-region.** v2.
- **Language-level determinism enforcement** — impossible in the agent languages;
  documented as a rule (P11), linted where feasible, not enforced.
- **A general-purpose workflow DSL.** Tape is purpose-built for *agent* runs and
  rides ADK's loop; it does not replace it.
- **History compaction** beyond the decision-boundary snapshot sketch in §6.

---

## 10. Open problems

| Problem | Where it bites | How Tape handles it (and what's left) |
|---|---|---|
| "Tools may run >1× on resume" (ADK) | every effect on every re-drive | the effect ledger + the `before_tool` short-circuit; **verify** the short-circuit return path behaves as documented across ADK versions |
| Replaying the *model* call so the re-drive gets the same decision | every decision on every re-drive | the `before_model`/`after_model` short-circuit; **verify** `LlmResponse` round-trips losslessly through Tape's JSONB |
| Replay drift from non-deterministic tool code | a tool that branches on `random()`/clock | unenforceable; documented (P11), linted where possible |
| `tape_*` history growth | long-lived sessions | decision-boundary snapshots (continue-as-new) — sketched, not built (v2) |
| The `AgentTool` plugin-callback gap | effects inside a sub-agent run under `AgentTool` | `TapeAgentTool` wrapper journals via the inner agent's own callbacks; **document** the limitation prominently |
| Reconciler needs a per-integration status check | true exactly-once on a given counterparty | the SDK's `status_check=` hook; without it, falls back to the counterparty's own idempotency window (bounded — treatise §IX ④) |
| Clock skew between Tape's lease and a slow agent process | a still-running run looks crashed | leases are renewed by the SDK on every RPC; the recovery loop only re-drives on a stale lease *and* no in-flight RPC |

---

## 11. Diagrams

New figures for this spec live alongside the treatise's, in `design-principles/`,
in the treatise house style (`tape_*.svg`):

| File | What it shows |
|---|---|
| [`tape_stack.svg`](tape_stack.svg) | the layered stack with Tape inserted beneath ADK |
| [`tape_protocol.svg`](tape_protocol.svg) | the `tape.proto` RPC surface, grouped by ledger |
| [`tape_state_capture.svg`](tape_state_capture.svg) | the six state kinds, each written in one txn with its ADK event — what gets written, when, where |
| [`tape_seq_alignment.svg`](tape_seq_alignment.svg) | the re-drive's `RecordDecision`/`BeginEffect` call stream matching the recorded `seq` stream until the first gap = the resume point |
| [`tape_effect_lifecycle.svg`](tape_effect_lifecycle.svg) | `BeginEffect` → write `pending` (commit) → act → `CompleteEffect{confirmed|failed|unknown}` → `RegisterCompensation`; the resume short-circuit when already `confirmed` |
| [`tape_adk_wiring.svg`](tape_adk_wiring.svg) | "no changes" — `TapePlugin` + `TapeSessionService` + `LongRunningFunctionTool` hooked into `Runner` / `SessionService` / tools |
| [`tape_resume.svg`](tape_resume.svg) | crash → ADK re-drives via `invocation_id` → Tape replays decisions + short-circuits confirmed effects + reconciler resolves a `pending`/`unknown` → run completes once |
| [`tape_languages.svg`](tape_languages.svg) | one Tape server, four SDKs, the proto contract in the middle |
| [`tape_before_after.svg`](tape_before_after.svg) | the 7-point ceremony vs the `@tape.effect` decorator |

---

## 12. Pluggable stores and horizontal scaling

The store is chosen by **URL at deploy time** — that is the whole "wiring". The
server reads `TAPE_STORE` (or `--store`), parses the scheme, builds the matching
store, migrates it, serves. Nothing above the server moves: the gRPC contract,
the SDKs, the agents are all unaffected.

| `TAPE_STORE` | backend | use |
|---|---|---|
| `sqlite:./tape.db` | file-backed SQLite (pooled, WAL) | the default; single-node, dev, small prod |
| `sqlite::memory:` / `memory` | ephemeral in-process SQLite | tests, demos |
| `postgres://user:pass@host:5432/db` | pooled PostgreSQL | production / horizontally scalable |
| `alloydb://user:pass@host:5432/db` | AlloyDB (PostgreSQL-wire-compatible) — run the AlloyDB Auth Proxy and point at `127.0.0.1:5432`, or a private-IP host | production / Google-managed Postgres at scale |
| `bigtable://project/instance/table` | Cloud Bigtable (`BIGTABLE_EMULATOR_HOST` honoured) | very-high-scale / GCP-native (see "the async substrate" below) |

Architecturally, Tape's *logical operations* — begin a run, record a decision,
begin/complete an effect, register a compensation, charge a budget, append an
event, … — are a trait, `RunStore`; everything else is plumbing. The SQL
backends (`SqlRunStore` over a `SqlBackend` — pooled `rusqlite` for SQLite,
pooled `postgres` for PostgreSQL **and AlloyDB**, which is wire-compatible)
share one set of portable SQL written once. A non-SQL backend implements the
same `RunStore`: the Cloud Bigtable backend (`tape/server/src/store/bigtable.rs`)
maps every operation onto single-row atomic mutations, with the per-run `seq`
held on the run row and bumped read-then-write (single-writer per run is
guaranteed by the lease, so no compare-and-swap is needed — Bigtable's
`ReadModifyWriteRow` would be tidier but the data-plane client crate doesn't
expose it; `CheckAndMutateRow` is available if you do need a conditional
mutation). Two Bigtable facts of life are documented and accepted there: the
table and its column family `m` must exist before startup (Bigtable requires
explicit table creation, like creating a Postgres database —
`cbt createtable tape && cbt createfamily tape m && cbt setgcpolicy tape m maxversions=1`),
and the `AppendEvent` two-write (the session row, then the event row) isn't a
single transaction (Bigtable has no cross-row transactions) — a crash between
leaves the state applied without its event, which the re-drive re-creates
idempotently. `BIGTABLE_EMULATOR_HOST` is honoured, so `bigtable://demo/demo/tape`
runs against the local emulator; the integration test
(`tape/tests/test_bigtable.py`) drives the treasury kill-and-resume scenario
against it — one wire, one GL batch, regardless of where the crash landed —
exactly the SQLite/Postgres test, against Bigtable.

**Horizontal scaling.** With a network store (Postgres), the Tape server is
**stateless between requests** — so you run *N* replicas behind a load balancer
and scale freely (`kubectl scale deploy/tape --replicas=N`, an HPA, `docker
compose up --scale tape-server=N`; see `tape/deploy/k8s/tape.yaml` and
`tape/docker-compose.yml`). Three properties make that safe with no extra
coordination:

1. **The lease.** `tape_runs` carries `lease_owner` + `lease_expires_at_ms`.
   `BeginRun`/`ResumeRun` take the lease with a conditional `UPDATE`; the
   recovery loop only re-drives a run whose lease is stale. So "one driver per
   run at a time" holds across replicas — and if a replica dies mid-drive, the
   lease expires and another picks it up.
2. **Idempotent RPCs.** Every mutating RPC carries `(run_id, seq)` (or the
   decision-derived effect key) and a replay returns the recorded row. So even if
   two recovery workers race and both re-drive a run, the loser short-circuits —
   no double wire, no double GL record. (This is the same property that makes the
   *re-drive* safe; it's reused here for free.)
3. **Single-step writes commit independently.** Each journal step is its own
   committed write (intent-before-effect is just an `INSERT` that commits before
   the tool runs); the only multi-row transaction is `AppendEvent` (the ADK
   event + the session state delta), which the `Store::tx` method does in one
   transaction on whichever backend.

The lease is the *only* coordination primitive, and it lives in the store, not
in the server — so the servers don't talk to each other.

### …and where this goes next — the async substrate (v2)

The reactors that drive recovery and reconciliation are, today, pollers
(`ListRunsToRecover`, periodic reconcile). The v2 axis turns them into
**event-driven reactors**: every mutating RPC also publishes to a topic (run
became RUNNABLE, effect went UNKNOWN, signal delivered) on a `EventBus` —
in-process by default, **Pub/Sub** in prod — and the recovery loop, the
reconciler, and the compensator become independent at-least-once subscribers
(idempotent already, by the property above). Paired with that, a **Bigtable**
`Store` (row key `run_id#seq`, one atomic single-row mutation per step;
`CheckAndMutate` for the effect-key dedup; the materialized current-state view
maintained by a reactor, or a small Spanner table for the read-your-writes hot
spots — run status, lease, budget admission), Cloud Tasks for the "wake me when
this lease/timer expires" delays, and an `grpc.aio` SDK that `await`s Tape on the
right side of the line (sync on the floor — `BeginEffect`, `AdmitBudget` — async
and batched everywhere else). `tape.proto` is frozen through all of this; the
`Store` trait and an `EventBus` trait are the seams. (`SubscribeRun`, already in
the proto, is the same fan-out, externalized.)

---

## 13. Relationship to the treatise, in one table

| Treatise | This spec |
|---|---|
| §I "answer vs act" | Tape is for agents that *act* — the journal is for the act, not the answer |
| §II the four-framework treasury walk; "#### ADK" | §1 (the "before"), §8 (the "after") |
| §III three commitments / three ledgers | P1 — decision / effect / obligation ledgers (§4, §6) |
| §VI "the idempotent-API defence" | P2 (decision-named keys), the reconciler (P3) |
| §VII "the reactive defence" | P5 (gates as durable suspends, not polled flags) |
| §VIII "what the new layer actually does" + "how replay actually works" | §3 (the ADK wiring), §6.5 (resumability), P8 |
| §IX six primitives of the ceiling | ①②③④⑤ are v1 (this spec); ⑥ is v2 |
| §X "the loop, made durable" | §6.5 — re-drive + `seq` alignment is that loop, expressed through ADK |
| §XI "on substrate" | §4 — the server is Rust, the agent stays in its own language |
| §XII "the landscape" | §9 non-goals (Temporal/Restate backends behind `tape.proto` — v2) |
| §XIII coding harnesses as proto-journals | the design target: turn the proto-journal into a real one |
| §XIV "what this treatise is glossing over" | §10 open problems, P11 |
| §XV "floor and ceiling" | P10 — plug into the floor (Postgres, ADK sessions, counterparty keys), build the ceiling |
