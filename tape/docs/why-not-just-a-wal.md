# Why not just a WAL?

A WAL is a chronology. Tape is a runtime. The chronology is necessary,
but it is not sufficient.

> A WAL answers: **what happened?**
> Tape answers: **what is true now? what is ambiguous? what must replay?
> what must never replay blindly?**

This page is for the engineer who has built a write-ahead log before and
wants to know what Tape adds. The short answer is: the same four things
durable-execution literature has been adding for forty years, plus one
thing the agent setting forces — `UNKNOWN` as a first-class outcome.

## What a WAL gives you

The classic WAL contract:

1. **Append-only.** Every interesting event is written before it takes
   effect.
2. **Ordered.** A monotonic sequence number identifies each entry.
3. **Crash-safe.** A process that dies after appending can be replayed.

That is the floor. Postgres has one. SQLite has one. Kafka is one.

For a pure CRUD service, a WAL plus a single-writer process is enough.
You replay, you apply, you converge.

## Why an agent runtime needs more

An agent is not a single-writer process and the world is not a CRUD
store. Three properties break the WAL-alone assumption:

### 1. The world has a third outcome

A SQL `INSERT` either commits or it doesn't. A wire transfer can
**commit**, **fail**, or **disappear into the network and never tell
you which**. The classic WAL records the *intent* and the *result*; it
has no story for the case where you have an intent and *the upstream's
status is unknown*.

Tape elevates `UNKNOWN` to a top-level effect status:

```text
EFFECT:
  PENDING    — we wrote the intent; not yet attempted
  RUNNING    — dispatched; awaiting result
  CONFIRMED  — upstream said "yes"
  FAILED     — upstream said "no"
  UNKNOWN    — dispatched; never got a useful answer
  ABSENT     — upstream looked up by business key, found nothing
  DUPLICATE  — upstream looked up by business key, found two
  STUCK      — repeated reconciliation gave no useful answer
```

A WAL with only `intent` and `result` would have to model `UNKNOWN` as
either an exception (lost) or a special result value (foot-gun). Tape
makes it the third axis of the state machine. See
[UNKNOWN](concepts/unknown.md).

### 2. Replay must distinguish "read" from "call"

In a CRUD WAL, replay means *re-apply the writes*. The writes are
idempotent in the store (`PUT key=value` twice is fine).

In an agent, "the writes" are model calls and tool calls. Re-calling
the model is a waste of money and an invitation to non-determinism.
Re-calling the tool is a wire transfer in the wrong direction.

Tape's replay is **passive on previously-confirmed effects** and
**active only at the resume point**:

> The first run makes calls. Replay makes reads.

A pure WAL would have to add a side-table to remember which effects
were confirmed and skip them on replay. Tape's projections *are* that
side-table, derived directly from the journal — which is the right
shape for the same data.

### 3. Obligations cross the WAL boundary

When a non-idempotent act commits and the run later decides it
shouldn't have (budget exceeded, gate denied, downstream rejected),
the world cannot be "unwritten" by truncating the log. The act must
be **compensated** — a new, distinct action that undoes the first.

A WAL can record "we wrote X." It cannot, on its own, say "we wrote X
and we still owe the world the cleanup." That is an obligation. It
needs:

- a place to live (the obligations projection),
- a process that watches it (the compensation reactor),
- a clear lifecycle (`PENDING` → `COMMITTED` → `COMPLETED`).

See [Compensation & sagas](concepts/compensation.md).

## Chronology vs. semantics

The cleanest way to keep the WAL/runtime distinction in mind:

| | The WAL | The runtime |
|---|---|---|
| **Shape** | A sequence of facts. | A small number of state machines over those facts. |
| **Question** | *What happened, in what order?* | *What is true now, and what must we do about it?* |
| **Author** | Append-only by the journal. | Authored by reactors, each under a lease. |
| **Recovery** | Re-apply. | Re-project, then resume. |

You can build the runtime *on top of* a WAL. You cannot replace it with
one.

## Event sourcing vs. durable execution

A reasonable question: *isn't this just event sourcing?* The shapes
look similar — append-only event log, state derived from replay.

The differences are operational, not theoretical:

- Event sourcing typically assumes **deterministic command handlers**.
  Tape's "command handlers" include an LLM, which is the least
  deterministic component in the system. Tape's contract is: the
  *outputs* of non-deterministic computations are journalled
  (`tape.sample`, recorded decisions), so replay is still
  deterministic *given the journal*.
- Event sourcing typically assumes the **store is the system of
  record**. Tape assumes the **upstream is the system of record** for
  acts that touched the real world. The journal records intent and
  the upstream's response; reconciliation is the loop that keeps the
  two consistent.
- Event sourcing rarely models `UNKNOWN`, leases, or obligations as
  first-class. Tape does — because that is the shape an agent
  runtime forces.

You can implement Tape on an event-sourcing framework. The framework
will not, on its own, solve the three problems above.

## Where the WAL still wins

There are pure-WAL workloads where Tape is overkill:

- Read-only agents (search, summarisation, retrieval).
- Agents that only make *idempotent* acts and don't care about
  replaying decisions (e.g., a pure pipeline).
- Workloads where the upstream is fully transactional and the model
  is not in the loop.

If you are in one of those buckets, a plain WAL — or even a
checkpoint-on-success retry loop — is enough. The moment a
non-idempotent act enters the picture, you are paying for Tape's
machinery whether or not you call it that. Better to use a runtime
that names the parts.

## The phrase to remember

> The WAL tells you what happened. The projections tell you what is true now.

The WAL is a fact. The runtime is a story. Both have to be true. Only
one of them is enough.

## Next

- [Architecture](architecture.md) — where the WAL sits in the system.
- [The journal](concepts/journal.md) — what the WAL contains.
- [Replay & resume](concepts/replay.md) — why replay is not retry.
- [UNKNOWN — the third outcome](concepts/unknown.md) — the part the
  WAL cannot tell you on its own.
