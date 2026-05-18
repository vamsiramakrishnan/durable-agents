# Systems path

For: the engineer who wants to understand **why** Tape looks the way it
does — the argument, the primitives, the trade-offs against Temporal /
LangGraph / DBOS, the math of replay.

Total time: a few hours, mostly reading the treatise. Worth it if you're
going to make architecture decisions on top of (or against) the runtime.

---

## 1 · The argument, end-to-end (60 min)

Read the treatise first. It's long, but it builds the model from first
principles and §IX is the contract every other doc page references:

→ **[Treatise (long-form)](../design/agents-that-act-treatise.md)** —
the foundational essay; "agents act on the world, and that requires a
runtime"

If you'd rather skim:

→ **[Rhythmic treatise](../design/agents-that-act-rhythmic.md)** — the
shorter, more lyrical version; same argument

The §IX primitives are the contract you'll see everywhere else:

* **decision** — the model call's recorded answer
* **effect** — an external action's intent + result (PENDING /
  CONFIRMED / FAILED / **UNKNOWN**)
* **obligation** — a registered compensation
* **gate / signal** — a parked run waiting for a human or a deadline
* **value** — reactive shared KV (the "coordinate through state" surface)
* **timer** — server-side fire-at-ms callback
* **run** — the lifecycle envelope around all of the above

---

## 2 · The journal vs. the projections (20 min)

The single mental model: one append-only WAL, several semantic projections
on top.

→ **[The journal (concept)](../concepts/journal.md)** — what's in it,
what isn't, why payloads are opaque JSON

→ **[Why not just a WAL?](../why-not-just-a-wal.md)** — what the
projections (effect ledger, obligations, runs, timers, KV) buy you
over plain WAL replay

→ **[Tape spec](../design/tape.md)** — the formal wire contract (the
proto, the state machines, the §12 concurrency invariants)

---

## 3 · Replay — the hardest concept (20 min)

→ **[Replay & resume (concept)](../concepts/replay.md)** — what
re-driving an agent actually does; idempotency-key derivation; the
short-circuit on `BeginEffect`

If you want it visually:

```bash
tape demo crash-resume --keep
tape inspect <run-id> --replay      # the FIRST RUN vs REPLAY screen
```

→ **[The replay diff (Inspector)](../how-to/inspect.md#the-replay-diff-r)** —
the in-vitro proof

The replay screen's per-row pair is the contract you read in §IX, made
clickable.

---

## 4 · The event bus rebuild (15 min)

A late addition that turned the WAL into a real event bus — subjects,
CEL predicates, server-managed reactions:

→ **[Event bus spec](../design/tape-event-bus.md)** — subject patterns
(`/tape/<kind>/<verb>/<dim>/...`), CEL predicates, the reactions table,
why this isn't Kafka

---

## 5 · Compared to the alternatives (15 min)

Where this lives in the durable-runtime landscape:

→ **[Tape vs. alternatives](../concepts/alternatives.md)** — the prose
comparison: ADK, AWS Step Functions, LangGraph, DBOS, Temporal

→ **[Parity matrix](../design/parity.md)** — the table: row-by-row
mapping of primitive to system

The shortest answer: **Temporal for stochastic orchestrators**. The
non-idempotent contract + the UNKNOWN status are what distinguishes
Tape from "Temporal but for agents".

---

## 6 · Chaos & failure (20 min)

How we test that the contract actually holds:

→ **[Chaos & failure testing](../design/chaos.md)** — LDFI-style fault
injection, the deterministic-replay madsim runner, the cross-SDK parity
harness

→ **[Cross-SDK parity (how-to)](../how-to/cross-sdk-parity.md)** — one
scenario, four languages, identical journal projection on every PR

---

## 7 · The proto is the contract

→ **[CLI reference](../reference/cli/index.md)** — the verbs

→ **[Python client](../reference/python/client.md)** — the gRPC stub
methods, one-to-one with the proto

Every SDK speaks the same proto; every server backend implements the
same `RunStore` trait. If something doesn't reconcile to a primitive
in §IX of the treatise, it's drift.

---

## What's *not* on this path

* The "how do I deploy this" pages. That's the **[Operator path](operator.md)**.

* The quickstart. Worth a glance just so you know what the user
  experience is, but the **[Beginner path](beginner.md)** is where it
  lives.
