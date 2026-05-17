# The journal

The journal is the source of truth. Everything else — the agent process,
the reactor processes, the upstreams — is replaceable.

## What gets recorded

Per run, in append-only fashion, on each turn:

| Record | When | Why |
|---|---|---|
| **Decision** | The agent picked a tool / produced an output | So replay doesn't re-ask the model |
| **Effect (begin)** | A tool body is about to run | So we know an act may have happened, even if we crash before it returns |
| **Effect (complete)** | A tool body returned (or failed, or UNKNOWN) | Source of truth for "did this act commit?" |
| **Budget admit + charge** | Before & after costed boundary | Spent counters survive crashes |
| **Gate** | The agent suspended waiting for a signal | The reactor knows the run is intentionally idle |
| **Signal / timer** | An external event arrived (or fired) | Replay can fast-forward to the next decision |
| **Obligation** | A non-idempotent act committed when it shouldn't have | The compensation reactor has work to do |

Each record carries `(run_id, seq, decision_index, ts)`. The journal is
strictly ordered by `seq` within a run.

## Three ledgers, separated

Tape splits state into three pieces so each has the right durability
properties:

- **The journal** — append-only history of decisions and effects. Source
  of truth for replay.
- **The session** — ADK's session state, snapshotted at decision boundaries.
  Used to reconstruct the agent's view of the world.
- **The KV** — the reactive shared-state primitive (`tape.set_value` /
  `watch_value`), CAS-versioned. Used for live coordination *between
  runs* and *between agents and humans*.

Mixing these is a footgun. The journal is not for live state. The KV is
not for history.

## The resume point

When the agent re-attaches after a crash, the resume point is the **first
`seq` that has no record** in the journal. Confirmed effects are short-circuited
(their result is replayed from history); pending effects are re-evaluated by
the reconciler reactor; decisions already made aren't re-asked.

```mermaid
sequenceDiagram
  participant A as Agent
  participant J as Journal
  participant U as Upstream
  A->>J: record_decision(seq=4, "wire_money")
  A->>J: begin_effect(seq=5, wire, idem_key=...)
  A->>U: bank.wire(..., idempotency_key=...)
  Note over A: 💥 crash
  Note over J: seq=5 is begin only,<br/>seq=6 (complete) missing
  Note over A: recovery reactor re-drives
  A->>J: list seq > 0
  J-->>A: ..., seq=5 begin (PENDING)
  A->>J: short-circuit if CONFIRMED;<br/>else reconciler resolves
```

## Storage

The journal is one logical table. The shape adapts to the backend:

- **SQLite / Postgres / AlloyDB** — one row per record, indexed on
  `(run_id, seq)`.
- **Bigtable** — one row per run, one cell per record. Per-row mutations
  are atomic — the journal's design respects that.
- **Spanner** (experimental) — like AlloyDB but globally consistent.

Switch backends by changing `TAPE_STORE`. The wire protocol doesn't move.
See [stores](../stores.md).

## What's not in the journal

- The model's prompt and completion bodies. The journal records the
  *decision* (which tool, with what arguments, against what
  `policy_version`). The full transcript lives in the ADK session.
- The full payload of a non-idempotent effect (only the digest +
  business key). The reactor reconstructs the payload on dispatch.
- Anything you put in the KV. The KV is its own thing.

## Why this matters

Once you accept "the journal is the source of truth," the whole rest of
the system falls out:

- **Why is recovery a reactor and not in the agent?** Because the agent
  is dead; the journal is still there.
- **Why does `BeginEffect` come before the tool body?** Because if the
  tool body runs and we crash before recording, the journal lies about
  the world.
- **Why does the journal need to be on a transactional store?** Because a
  half-written journal is worse than no journal.

Next: [Effects & idempotency](effects.md).
