# Runtime vs. framework

A choice of words that turns out to be a choice of architecture.

> A **framework** is an orchestration surface. You write code into the
> shapes the framework provides; the framework decides when your code
> runs.
>
> A **runtime** is a durable execution substrate. You write the code in
> whatever shape you want; the runtime decides what survives a crash,
> what replays, what is allowed to run twice, and what is not.

Tape is the second thing. The agent framework (ADK) is the first. They
do not compete. They compose.

## The two layers, made explicit

```text
   ┌────────────────────────────────────────────────────────────┐
   │   Agent framework  — ADK, LangGraph, CrewAI, Pydantic AI   │
   │                                                            │
   │   • prompts, tools, agents-as-objects                      │
   │   • orchestration surface ("a workflow", "a graph")        │
   │   • developer ergonomics                                   │
   └────────────────────────────────────────────────────────────┘
                              │
                              │   "what to run"
                              ▼
   ┌────────────────────────────────────────────────────────────┐
   │   Durable runtime — Tape, Temporal, Restate, DBOS,          │
   │   Step Functions, Cloud Workflows                          │
   │                                                            │
   │   • journal, replay, leases, timers, signals               │
   │   • exactly-once-effective dispatch                        │
   │   • compensation, obligations, UNKNOWN as a state          │
   │   • survives process / pod / region failure                │
   └────────────────────────────────────────────────────────────┘
```

The agent framework decides *what the agent does*. The runtime decides
*what is true about the agent's actions, across crashes*.

## Where the alternatives sit

| | Layer | Surface | Notes |
|---|---|---|---|
| **Tape** | runtime | gRPC + four SDKs | scoped to ADK; ships UNKNOWN, outbox, compensation, leases as first-class |
| **Temporal** | runtime | workflow code in your language | mature; general-purpose; agent contracts are yours to build |
| **Restate** | runtime | "durable handlers" with virtual objects | event-driven; agent contracts are yours to build |
| **DBOS** | runtime | Python decorators on a Postgres-backed engine | Postgres-only; same conclusion as Tape, different path |
| **Step Functions / Cloud Workflows** | runtime | declarative state machine JSON/YAML | strong on AWS / GCP integrations; less ergonomic for agent code |
| **LangGraph** | framework (+ checkpointing) | Python graph of agents | great for fixed-shape agents; checkpoints are coarse |
| **CrewAI** | framework | Python crews-and-tasks | no durable runtime story today |
| **Pydantic AI** | framework | Python "agent" objects | composes with DBOS to add a runtime |

The split isn't always clean — LangGraph ships checkpointing, ADK ships
session state, Pydantic AI ships a runtime story via DBOS — but the
layering is the right way to compare them.

## Why the distinction matters

The cost of confusing the two layers is borne in production:

1. **A framework that *thinks* it is a runtime** will lose your acts.
   It will retry on exceptions, double-call non-idempotent upstreams,
   and offer no answer to "what is true now?" when the process dies in
   the middle.
2. **A runtime without an opinionated framework** will run anything,
   but the cost of integrating each new agent shape is paid by you.
   Every team rebuilds the same callback adapters.

The right combination is a thin framework (or framework adapter) over
a sharp runtime. Tape is the runtime, scoped tightly to ADK so the
adapter is small.

## Tape's specific bet

Tape's bet, in three pieces:

- The **agent framework** should be your problem to choose. ADK is what
  Tape supports today; the protocol is language-agnostic and the
  adapter is the only ADK-specific surface.
- The **runtime** is one component — separable, replaceable, ops-honest.
  You can scale the Tape server independently of your agent. You can
  swap the store backend. You can run reactors on a different cluster.
- The **contract** between them is the journal. Not session state, not
  graph state, not framework hooks — a journal of decisions and
  effects. Everything else is derived.

That bet is *not* "Tape is the only runtime." It is "the layer below
your agent framework should look like a runtime, with a journal at the
centre, and Tape is what that looks like for ADK today."

## When the framework is enough

If your agent makes only **idempotent**, **cheap**, **read-only** calls
— and you don't care whether two replays double-issue them — you do not
need a runtime. The framework's retry loop is enough. Most pure-search
or pure-RAG agents are in this bucket.

The moment you have a non-idempotent act, a budget that must not be
double-spent, or a multi-step trajectory where the first step has
already touched the world, you need a runtime. Not Tape specifically —
a runtime. Tape is one answer; the comparisons above are the others.

## When Tape is the right runtime

- You're on **ADK** today.
- You have **non-idempotent** acts (wires, tickets, sends, API calls
  without server-side idempotency keys).
- You want to deploy on **GCP** with reasonable defaults (Cloud Run,
  AlloyDB, Pub/Sub) and a Terraform/Helm path that you can read and
  edit.
- You value **opinionated primitives** over feature-completeness —
  UNKNOWN, outbox, compensation, leases, reactive KV, all named and
  enforced.

The honest counterpoints — multi-region, mature web UI, search
attributes, replay-testing tooling — are on the
[parity matrix](design/parity.md).

## Next

- [Tape vs. alternatives](concepts/alternatives.md) — the executive
  summary, by alternative.
- [Architecture](architecture.md) — what the Tape runtime is, in one
  page.
- [The treatise](design/agents-that-act-treatise.md) — the
  long-form argument for why the layer needs to exist.
