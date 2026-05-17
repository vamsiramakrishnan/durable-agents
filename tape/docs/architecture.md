# Architecture

Tape is **one execution journal** with **semantic projections** and a set of
**reactors** that close the gap between what the journal says happened and
what the world says happened. This page is the canonical engineering
explanation. Read it once; cross-reference it later.

## The system, in one diagram

```text
┌────────────────────────────────────────────────────────────────────┐
│                         ADK Agent Code                             │
│                                                                    │
│   app = App(..., plugins=[TapePlugin(...)])                        │
│   runner = Runner(..., session_service=TapeSessionService(...))    │
│                                                                    │
│   @tape.effect(...)                                                │
│   def tool(ctx, ...):                                              │
│       ...                                                          │
└───────────────┬───────────────────────────────┬────────────────────┘
                │                               │
                │ control-flow durability       │ session durability
                ▼                               ▼
┌─────────────────────────────┐     ┌────────────────────────────────┐
│        TapePlugin           │     │     TapeSessionService         │
└───────────────┬─────────────┘     └───────────────┬────────────────┘
                └──────────────┬────────────────────┘
                               ▼
                    ┌────────────────────┐
                    │    Tape Server     │
                    │  (Rust · gRPC)     │
                    └─────────┬──────────┘
                              │
       ┌──────────────────────┼──────────────────────────┐
       ▼                      ▼                          ▼
 ┌───────────┐         ┌───────────┐               ┌───────────┐
 │  RunStore │         │   WAL     │               │ Reactive  │
 │ (journal) │         │ (tail)    │               │    KV     │
 └─────┬─────┘         └─────┬─────┘               └───────────┘
       │                     │
       │   ┌─────────────────┼─────────────────┐
       ▼   ▼                 ▼                 ▼
 ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
 │ recovery │ │reconciler│ │  outbox  │ │  timers  │
 └──────────┘ └──────────┘ └──────────┘ └──────────┘
                                              │
                                              ▼
                                       ┌──────────────┐
                                       │ compensation │
                                       └──────────────┘
```

The agent process is replaceable. The reactor processes are replaceable.
The upstreams are replaceable. The journal is the only piece that is not.

## The mental model

> Tape = **execution journal** + **semantic projections** + **recovery
> state machines**.

Not "three databases." One append-only log of facts, and a small number
of derived views that answer different operational questions. Mix them up
and you will design something worse than what already exists.

### The execution journal

One append-only record per run. Strictly ordered by `seq` within a run.
Each record carries `(run_id, seq, decision_index, ts)`. The record kinds
are listed in [The journal](concepts/journal.md).

The journal is the **only** authoritative answer to *what happened?*. It
is not the answer to *what is true now?* — projections answer that.

### Semantic projections

The journal is a chronology. Projections are the meanings the runtime
extracts from that chronology:

| Projection | Reads from the journal | Answers |
|---|---|---|
| **Decisions** | recorded model choices | Where did the run choose to go, and why? |
| **Effects** | begin/complete records, with confirmation status | Did this act commit? Is it `UNKNOWN`? Is it stuck? |
| **Obligations** | committed acts the run later decided shouldn't have happened | What does compensation need to undo? |
| **Timers** | scheduled wake-ups (`gate_timeout`, `redrive`, `reconcile`) | What is the next thing this run is waiting for? |
| **Gates** | suspend-until-signal records | Is the run intentionally idle? Who must signal it? |
| **Budgets** | admit + charge pairs | How much of the cap is left? |
| **Reactive KV** | versioned set/CAS records on `(namespace, key)` | What is the *current value* — and what *changed* to get here? |

The phrase to repeat to yourself:

> The WAL tells you what happened. The projections tell you what is true now.

### Recovery state machines (reactors)

The journal alone doesn't move. Reactors do. They read the journal, find
gaps between it and reality, and close them — under leases, idempotently.

The five reactors are documented in [Reactors](concepts/reactors.md). The
shape is always the same: *find work · acquire lease · do work · update
state · heartbeat · release*.

## Failure-first explanation

The right way to understand Tape is to ask, at every point in the agent's
execution: *what happens if the process crashes here?*

| Position in the loop | What survives | What replays | What does not replay |
|---|---|---|---|
| Before a decision is journalled | Nothing about this turn | The whole turn | — |
| After decision journalled, before effect begin | The decision | Decisions up to here; first run for the effect | The model call (it's a recorded decision now) |
| After effect `BeginEffect`, before tool body runs | The intent | Decisions; the reconciler may attempt `observe()` | — |
| After tool body runs, before completion is journalled | The intent (status = `PENDING` / `UNKNOWN`) | Decisions; the reconciler resolves the outcome | The tool body, if the upstream is non-idempotent |
| After effect `CONFIRMED` | The result | Decisions; the recorded result is handed back | The tool body |
| Inside a gate | The waiting state | Replay reconstructs the wait; signal/timeout drives it | The model call(s) that led to the gate |

Every line in that table is enforced by a journal record. None of it
relies on Python's heap.

## Why replay is not retry

A retry decorator re-runs the function. A replay reconstructs the
function's view of the world from history and runs only the parts that
were never durably recorded.

The shorthand:

> **Retry repeats the story. Resume remembers the story.**
> **The first run makes calls. Replay makes reads.**

The full mechanics are in [Replay & resume](concepts/replay.md).

## Why leases are required

Durability creates replay races. Without coordination, two recovery
workers can both observe a stale lease and both decide to re-drive the
same run, racing each other into the same upstream.

A **lease** is the temporary authority to extend the journal. It is
not a lock on the data; it is a lock on *who may write the next entry*.
Two workers can read the journal at once. Only one may extend it.

> The journal preserves execution history.
> The lease grants temporary authority to extend that history.

See [Leases](leases.md) for the CAS shape and the takeover rules.

## Why a WAL alone is not enough

Tape's journal is a WAL, but the WAL is one input. The runtime *also*
needs:

- the **third outcome** of every effect (`UNKNOWN`) recorded as a
  first-class state, not as an exception;
- **obligations** — the trace that ties a committed-but-unwanted act to
  its compensating action;
- **reconciliation** — the loop that turns `UNKNOWN` into
  `CONFIRMED`/`ABSENT`/`DUPLICATE` by *asking the upstream*, not by
  guessing;
- a **lease model** that survives across replicas without coordination.

A pure WAL tells you what your *process* did. Tape tells you what the
*system* is, including the parts you cannot see from inside the process.
See [Why not just a WAL?](why-not-just-a-wal.md).

## ADK integration

Tape ships two ADK adapters and a one-call wiring helper:

```python
from tape.adk import durable_app
app, runner = durable_app(name="treasury", agent=root_agent)
```

Under the covers:

- `TapePlugin` is registered on the `App`. It rides
  `before_model_callback` / `after_model_callback` to journal decisions,
  and `before_tool_callback` / `after_tool_callback` /
  `on_tool_error_callback` to journal effects.
- `TapeSessionService` is registered on the `Runner`. It mirrors ADK's
  session events into Tape as part of the same transaction.
- Resume uses ADK's `invocation_id` — the agent process re-attaches by
  invocation, and Tape replays from `seq=0` to the resume point.

No ADK changes are required. See [ADK on Tape](adk.md).

## Topology

The system has four moving parts. Deploy them independently:

```text
 client (any SDK)
       │
       ▼
 ┌──────────────┐         ┌──────────────────┐
 │  ADK agent   │────────▶│   Tape server    │
 │  + Plugin    │  gRPC   │   (stateless)    │
 └──────────────┘         └────────┬─────────┘
                                   │
                                   ▼
                          ┌──────────────────┐
                          │   RunStore       │
                          │ (Postgres /      │
                          │  AlloyDB /       │
                          │  Bigtable /      │
                          │  SQLite)         │
                          └──────────────────┘
       ▲                           ▲
       │                           │
 ┌─────┴──────┐             ┌──────┴──────────┐
 │  reactors  │  re-drive   │  outbox + sinks │
 │ (sidecars) │────────────▶│  (Pub/Sub, …)   │
 └────────────┘             └─────────────────┘
```

The server is stateless between requests once the store is networked.
Run *N* replicas; the per-run lease in `tape_runs` keeps "one driver per
run at a time." See [Cloud Run topology](gcp-cloud-run.md).

## What's next

- [The journal](concepts/journal.md) — what gets recorded, and where.
- [Replay & resume](concepts/replay.md) — the resume point, in detail.
- [Leases](leases.md) — authority to extend the journal.
- [UNKNOWN — the third outcome](concepts/unknown.md) — the
  ambiguity protocol.
- [Reactors](concepts/reactors.md) — the loops that close the gap.
- [Why not just a WAL?](why-not-just-a-wal.md) — chronology vs.
  semantics.
- [Runtime vs. framework](runtime-vs-framework.md) — where Tape sits.
