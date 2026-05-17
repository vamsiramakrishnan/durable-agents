# Glossary

Every Tape-specific term, defined. If a doc page assumes you know a word
and it isn't on this page, file an [issue][gh-issues] — the docs need
to be self-contained.

[gh-issues]: https://github.com/vamsiramakrishnan/durable-agents/issues

## Core nouns

ADK
:   Google's **Agent Development Kit**. The host agent framework Tape is
    scoped to. Tape rides on ADK's plugin system, custom
    `SessionService`, `LongRunningFunctionTool`, and resumable
    `invocation_id`.

Agent
:   The thing that *acts*. In Tape, "agent" specifically means an ADK
    `LlmAgent` or composition of agents that calls tools. The agent
    process is separate from the runtime processes (server + reactors).

App
:   A named deployment unit. The `name=` you pass to `durable_app(...)`.
    Scoping unit alongside `user_id` and `session_id`.

Budget
:   Run-level dollars and tokens, journalled. `AdmitBudget` before a
    costed boundary, `ChargeBudget` after. Spent counters survive
    crashes.

Compensation
:   A *second* operation that undoes a *first*, committed operation.
    The saga pattern (Garcia-Molina & Salem, 1987). Tape's compensation
    reactor walks obligations LIFO over a run.

Connector
:   A typed adapter to one upstream. Implements three methods:
    `dispatch`, `observe`, `compensate`. The outbox / reconciler /
    compensation reactors call into it.

Decision
:   A choice the agent makes — typically "call tool X with args Y" or
    "produce output Z." Decisions are journalled and replayed on resume,
    so the agent doesn't re-ask the model for choices it already made.

Effect
:   A tool call that touches the outside world. Has a declared semantics
    (`IDEMPOTENT` / `NON_IDEMPOTENT` / `OBSERVE_ONLY`) and a status
    (`PENDING` → `CONFIRMED` / `FAILED` / `UNKNOWN`).

Gate
:   A `LongRunningFunctionTool` (ADK term) that suspends the run until
    a named signal arrives or a timer fires. `tape.gate_tool("name",
    timeout_ms=...)`.

Idempotency key
:   The deterministic identity of a tool call across replay. In Tape:
    `r-<run>/d-<decision>/<tool>/<call_idx>`. Derived from the
    journal, not from inputs.

Journal
:   The append-only history of decisions and effects per run. The source
    of truth. Replay reconstructs the agent's view from the journal.

Lease
:   A CAS-held claim by one reactor on one row (run / effect /
    obligation / timer). Time-bounded; the holder heartbeats. Other
    workers see the lease and skip.

Obligation
:   A pending compensation. Created when the reconciler detects a
    `DUPLICATE` or when `compensate_run` walks confirmed effects. The
    compensation reactor resolves obligations.

Outbox
:   The dispatch pattern for non-idempotent effects: tool body returns
    intent only; the outbox reactor performs the dispatch under a CAS
    lease. `@tape.outbox_tool(...)` is the decorator that opts in.

Policy version
:   A label (`"cfo-2026.05"`) journalled on every decision. The agent
    can branch on `tape.policy_is(tool_context, "...")`. Tape doesn't
    automatically use the new code path for new runs — you do.

Reactor
:   A background loop that closes a specific gap between the journal
    and the world. Five ship: `recovery`, `reconciler`, `outbox`,
    `timers`, `compensation`. Each is idempotent.

Reactive KV
:   Tape's CAS-versioned, watchable key-value store. `set_value /
    get_value / watch_value / delete_value`. *Not* the journal. Used
    for fan-out by key, multi-subscriber.

Replay
:   The act of reconstructing an in-flight run from `seq=0` to the
    resume point. Confirmed effects are short-circuited; recorded
    decisions are handed back to the agent loop.

Resume point
:   The first `seq` in the journal with no record. Where the agent
    actually starts *running* again on re-drive.

Run
:   One conversation / trajectory / session. Identified by `run_id`,
    typically derived from ADK's `invocation_id`. The unit of leasing,
    cancellation, and budget.

Saga
:   A sequence of operations where each step has a registered inverse
    (compensator). If a later step fails, earlier steps are compensated
    in LIFO order. Tape's compensation reactor implements this.

Signal
:   A named, point-to-point message to a specific run. `SendSignal` from
    outside; `AwaitSignal` from inside. Single-consumer.

Status check
:   A callable that asks the upstream — out of band — whether a specific
    effect committed. Used by the reconciler when an effect is `UNKNOWN`.

STUCK
:   A terminal-ish state for a run, effect, or obligation when Tape
    can't determine what happened. STUCK is **good** — the alternative
    is silently wrong.

Tape server
:   The Rust server that owns the wire protocol. Stateless; horizontally
    scalable; speaks one of SQLite / Postgres / AlloyDB / Bigtable /
    Spanner to the store.

Tool
:   ADK's primitive. Tape decorates tools with `@tape.effect` (idempotent
    upstream) or `@tape.outbox_tool` (non-idempotent upstream).

UNKNOWN
:   The third outcome. The request *might* have committed; we don't
    know. The reconciler reactor resolves it by calling `observe()`.

WAL tail
:   `SubscribeEvents` — a cross-run stream of journal entries ordered
    by `(ts, run_id, seq)`. Wire it through `run_event_fanout` for an
    in-process consumer or `run_outbox_relay` for an exactly-once-effective
    publisher.

## Verbs

cancel_run
:   Mark a run `CANCELLED`. Cooperative — the agent bails at the next
    boundary. Does **not** automatically compensate.

compensate_run
:   Walk a run's confirmed effects LIFO and enqueue compensation
    obligations for each. The saga rollback.

dispatch
:   The forward path of an effect: the connector talks to the upstream.

heartbeat
:   Extend the lease on a run from within a long tool body, so the
    recovery reactor doesn't decide the run is stale.

observe
:   The out-of-band lookup the reconciler uses to resolve `UNKNOWN`.
    The connector implements it.

redrive
:   "Wake this run up." Operator's poke-with-a-stick. Schedules an
    immediate `redrive`-kind timer for the recovery reactor.

reconcile
:   What the reconciler reactor does — walk `UNKNOWN` effects, call
    `observe`, write the resolved status back.

sample
:   `tape.sample(tool_context, fn)` — call `fn` once per run, journal
    the result, return-from-history on replay. The escape hatch for
    nondeterminism (wall-clock, random, external reads).

set_value / watch_value
:   Reactive KV operations. CAS-versioned writes; watch streams that
    emit transitions (`prev_value` → `value`).

## Three-letter words you'll see

ADC
:   Application Default Credentials. The Google Cloud auth source.

CAS
:   Compare-And-Swap. The lease primitive — write conditional on a
    previous version.

GSA / KSA
:   GCP Service Account / Kubernetes Service Account. Workload Identity
    binds the two.

KV
:   Key-Value (the reactive store).

OTel
:   OpenTelemetry. Tape opens spans on `tape.begin_run`,
    `tape.begin_effect`, etc.

SA
:   Service Account.

SDK
:   Software Development Kit. Python is the reference; Go / TypeScript /
    Java are wired clients with the protocol round-trip.

TTL
:   Time-To-Live. Leases have one; tokens have one.

WAL
:   Write-Ahead Log. The cross-run journal stream.

## Two-letter words you'll see

H2
:   HTTP/2. `tape-server` runs with `--use-http2` on Cloud Run.

IaC
:   Infrastructure as Code. Tape ships Terraform/OpenTofu + Helm.

## See also

- [Concepts](../concepts/index.md) — the same terms, in narrative form.
- [Cheat sheet](../reference/cheatsheet.md) — every API, one page.
