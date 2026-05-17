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

See [`tape/README.md`](tape/README.md) for the quickstart.

## License

[Apache 2.0](LICENSE).
