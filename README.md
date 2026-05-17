# Durable Agents — *Tape*

This repository has two halves.

## `design-principles/` — the argument

The treatise ***When the Orchestrator Isn't Code — a treatise on agents that act*** and its
companion essays and figures. The thesis, in one breath: when an LLM is the
orchestrator, an agent that *acts* needs a **journal underneath it** — recorded
decisions, idempotent effects, an explicit `unknown` outcome, action gates,
budgets-as-state, compensation, and replay-as-memory — and the agent-framework
layer should put a **durable runtime beneath the model**, not bolt ceremony on
top of it. Read [`design-principles/agents-that-act-treatise.md`](design-principles/agents-that-act-treatise.md)
first; [`design-principles/tape.md`](design-principles/tape.md) is the design
spec that turns the argument into a system.

## `tape/` — the substrate

**Tape** is that durable runtime, built as a *separate system*: a high-concurrency,
low-latency server (Rust, Postgres-backed) with a language-agnostic wire protocol
and SDKs that plug into **Google's Agent Development Kit (ADK)** *with no changes to
ADK* — riding only on extension points ADK already exposes (the plugin system,
custom `SessionService`s, `LongRunningFunctionTool`, and `invocation_id`-based
resume). Tape gives an ADK agent a journal: every model call recorded, every tool
call made exactly-once-effective, every gate a durable suspend-until-signal, every
budget a piece of run state, every irreversible step compensable — so a crashed run
*reconstructs* and continues instead of re-acting.

> Python will write the agent. Something else will run it. Tape is the something else.

## Mental model

```text
Decision Ledger
  = memory of reasoning

Effect Ledger
  = memory of reality

Obligation Ledger
  = memory of responsibility
```

Tape is not checkpointing Python processes.

Tape is:

```text
record every model decision
record every external effect intent/result
replay decisions
skip confirmed effects
stop on ambiguity
reconcile reality
compensate when reality disagrees
```

## Architecture guide

If you're new to Tape, read these in order:

1. [`tape/README.md`](tape/README.md) — quickstart and integration
2. [`tape/docs/architecture.md`](tape/docs/architecture.md) — execution model, ledgers, replay, leases, reactors, non-idempotent upstreams
3. [`design-principles/tape.md`](design-principles/tape.md) — the full design specification
4. [`design-principles/agents-that-act-treatise.md`](design-principles/agents-that-act-treatise.md) — the broader argument

The architecture guide includes:

- how replay works
- why the three ledgers exist
- why WALs alone are insufficient
- leases and recovery
- outbox + reconciliation for non-idempotent APIs
- the execution journal mental model
- ASCII diagrams for recovery and runtime semantics

See [`tape/docs/architecture.md`](tape/docs/architecture.md).

## License

[Apache 2.0](LICENSE).
