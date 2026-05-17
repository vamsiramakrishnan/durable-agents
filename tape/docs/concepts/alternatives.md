# Tape vs. alternatives

The honest comparisons. Tape isn't the only way to make an agent durable;
it's one specific bet on **where** the runtime should live and **what**
contracts it should enforce. The full parity matrix lives at
[**design / parity**](../design/parity.md); this page is the executive
summary.

## TL;DR

| You should look at | If you want |
|---|---|
| [**Tape**](https://github.com/vamsiramakrishnan/durable-agents) | A *substrate under ADK*, with the agent runtime + the durable execution primitives unified. UNKNOWN as a first-class status. Non-idempotency safety enforced at decoration time. |
| [**Temporal**](https://temporal.io) | A *general-purpose durable execution* engine, far more mature, with workflow code in your language. You'll build the agent-runtime layer on top. |
| [**LangGraph durable**](https://langchain-ai.github.io/langgraph/) | An agent-first runtime with checkpointing. Same layer as Tape, different opinions: graph-shaped agents, different idempotency story. |
| [**Pydantic AI + DBOS**](https://docs.dbos.dev/) | Agent + durable workflow, both from the same vendor. The runtime is more opinionated; the SDK is Python-only. |

## Tape vs. Temporal

The line: **Temporal is the floor; Tape is the ceiling.**

- Temporal solves durable execution as a general primitive. Workflows are
  written in your language, replayed by a worker, with timers / signals /
  child workflows / search attributes / a polished web UI.
- Tape solves agent-runtime concerns on top of durable execution: the
  third outcome (`UNKNOWN`), non-idempotency decoration-time safety,
  budget-as-state, ADK-native resume, the outbox-for-non-idempotent
  pattern, replay-as-memory for an LLM orchestrator.

You can build Tape on Temporal. The spec lists *Temporal/Restate engine
behind `tape.proto`* as v2. Today Tape's server is Rust + Postgres; that's
not because Temporal couldn't do the job, but because Tape ships with the
agent-runtime contracts baked in.

Pick **Temporal** if:

- Your problem is more general than "make my ADK agent durable."
- You want the mature web UI / search attributes / replay-testing tools.
- You're comfortable building the third-outcome / outbox / budget /
  policy-version primitives yourself.

Pick **Tape** if:

- Your agent uses ADK and you want the journal *underneath* it, with no
  framework code to wrap.
- You want UNKNOWN, non-idempotency safety, and budget-as-state out of
  the box.
- You value the smaller surface area and the explicit roadmap over the
  feature completeness.

The [parity matrix §1](../design/parity.md#1-tape-vs-temporal) has the
feature-by-feature audit, including the things Tape **doesn't** have
yet (continue-as-new, child workflows, search attributes, replay-testing
tools, sandbox-enforced determinism).

## Tape vs. LangGraph durable

The line: **same layer, different shape.**

- LangGraph models the agent as a **graph** with explicit nodes and edges.
  Checkpointing is per-node. Durability is on graph state.
- Tape models the agent as an **LLM-orchestrated trajectory** — there's
  no graph. The journal is per-decision and per-effect. Durability is on
  the journal.

The trade-off is observable: LangGraph's graph is great when the structure
is fixed up front. Tape's trajectory is right when the model is genuinely
choosing the path at each turn. Most "agents that act" — the kind that
move money, file tickets, book travel — are trajectory-shaped, not
graph-shaped.

[Parity matrix §2](../design/parity.md#2-tape-vs-langgraph-durable-execution)
covers the specifics.

## Tape vs. Pydantic AI + DBOS

The line: **same conclusion, different framework.**

Pydantic AI + DBOS arrived at the same answer Tape did — *agents need
durable execution underneath* — and built it on top of DBOS as the
runtime. The conclusion is the same. The differences are real:

- **Host agent framework**: Pydantic AI vs. Google's ADK. If you've
  picked ADK, Tape is the lower-effort path; if you've picked Pydantic
  AI, the inverse.
- **Runtime**: DBOS (Postgres-backed, Python-first) vs. Tape's Rust
  server. Different scaling and ops profiles.
- **Multi-language SDKs**: Tape ships Python + Go + TypeScript + Java
  clients today. Pydantic AI is Python-only.

[Parity matrix §3](../design/parity.md#3-tape-vs-pydantic-ai-dbos-dbosagent)
has the side-by-side.

## When Tape is the wrong choice

Be honest about this:

- **You're not using ADK.** Tape is scoped to ADK. The protocol is
  language-agnostic, but the host agent framework is ADK.
- **You don't have non-idempotent acts.** A pure read-only agent
  (search, summarisation, retrieval) gets less out of Tape's safety
  primitives. Worth using for the journal, but not the headline feature.
- **You need multi-region active-active today.** That's Spanner territory,
  and the Spanner backend is experimental.
- **You need a UI to manage runs.** Tape has the data
  (`SubscribeEvents`, `SubscribeRun`); it doesn't have the dashboard.
  Temporal Cloud does.

## Next

- [Parity matrix](../design/parity.md) — the feature-by-feature audit.
- [The treatise](../design/agents-that-act-treatise.md) — the
  argument for why the runtime should live where Tape puts it.
