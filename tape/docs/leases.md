# Leases

A **lease** is the temporary authority to extend a run's journal. It is
the only coordination primitive Tape relies on — and the only one it
needs.

> The journal preserves execution history.
> The lease grants temporary authority to extend that history.

## Why durability creates replay races

Once the journal is the source of truth, *any* process with a connection
to the Tape server can attach to a run and try to drive it forward. That
is the property that makes recovery work — a fresh pod, started after a
crash, can resume an in-flight run because the previous pod's heap is
not the source of truth.

It is also the property that makes multiple-driver races dangerous:

```text
   recovery worker A           recovery worker B
        │                            │
        │ list RUNNABLE              │ list RUNNABLE
        ▼                            ▼
     finds run R                  finds run R
        │                            │
        │       (no lease, no CAS)
        ▼                            ▼
   re-drives R              re-drives R
        │                            │
        ▼                            ▼
   wires money               wires money       ← bad
```

The fix is not "try harder to coordinate." The fix is to grant exactly
one writer at a time the right to extend the journal, and let everyone
else short-circuit harmlessly.

## What a lease is

A lease is a tuple stored on the run row:

```text
(owner_id, reactor_name, expires_at, fencing_token)
```

- `owner_id` — the process / pod that holds the lease right now.
- `reactor_name` — which reactor / driver took it (`recovery`,
  `reconciler`, `outbox`, `agent`).
- `expires_at` — when the lease becomes stale. Other workers may try to
  take over after this point.
- `fencing_token` — a monotonically increasing integer. Any write the
  lease holder makes carries the token; the store rejects writes with
  a stale token. This is the "STONITH" for split brain.

The lease is acquired via **CAS** on the run row. The store rejects an
acquire that doesn't match the expected `(owner_id, fencing_token)`.
The acquire is the only point of contention; everything downstream is
unblocked.

## Leases vs. locks

Two distinctions worth making explicit:

- A **lock** stops other readers. A lease does not — anybody may *read*
  the journal at any time. The lease only bounds *writers*.
- A **lock** is held until released. A lease *expires*. If the holder
  crashes, the world keeps moving: the lease times out, another worker
  takes over, the journal grows.

The combination — bounded writer, free reader, expiry on crash — is
what makes Tape's reactors safely multi-replica with no extra
coordination service.

## Lease lifecycle

```text
   worker A acquires lease
         │
         ▼
   run executing
         │
         │ ── heartbeat ── heartbeat ── heartbeat …
         ▼
       crash
         │
         ▼
   lease expires (TTL passes without heartbeat)
         │
         ▼
   worker B observes expired lease, CAS-acquires
         │
         ▼
   replay from seq=0 to resume_point
         │
         ▼
   continue from resume_point
```

The cycle is the same for every actor that extends the journal:
recovery, reconciler, outbox, the agent itself.

## Heartbeats

Long-running drivers must renew the lease before it expires. The SDK
takes care of this for `tape.effect` bodies; for outbox dispatch and
reactor work, the reactor framework bumps the lease on each tick.

If a heartbeat fails (lost connection, store gone), the driver must
**abandon the work in progress**. The lease will expire; some other
worker will pick up. Any write the abandoning driver attempts will be
rejected by fencing — so abandoning is safe.

## Takeover rules

Takeover is the moment a worker observes an expired lease and tries
to claim it. The rules:

1. **Read the lease** — note `expires_at` and `fencing_token`.
2. **Compute the new state** — `(my_owner, my_reactor, now + ttl,
   token + 1)`.
3. **CAS the lease** — succeed only if `(owner_id, fencing_token)` on
   the row still matches what we read. If not, somebody else
   already took over; back off.
4. **Replay the journal** — reconstruct the resume point. Confirmed
   effects are short-circuited; pending effects go through the
   reconciler.
5. **Continue from the resume point** — extend the journal under the
   new fencing token.

The store enforces fencing on every write — `AppendEvent`,
`CompleteEffect`, `WriteValue`. A stranded driver with a stale token
cannot corrupt the journal.

## Why this is enough

The lease is the only thing standing between "two workers race into the
same wire transfer" and "exactly one worker, at a time, extends the
journal." It works because the journal is the source of truth:

- The loser of a takeover race short-circuits harmlessly — it reads the
  same journal the winner is extending, and the next write will be
  fenced out.
- The winner doesn't need to know the loser even existed — replay is
  deterministic given the journal, so it starts from the same state
  regardless of who held the previous lease.
- A network partition that strands a driver does **not** strand the
  run — the lease expires, another driver takes over, and the journal
  keeps growing.

There is no need for a separate coordination service (ZooKeeper, etcd,
a leader-election library). The store *is* the coordinator. Every
RunStore backend (SQLite, Postgres, AlloyDB, Bigtable) supports the
CAS shape the lease needs.

## When the lease isn't enough

Two cases the lease cannot fix on its own:

- **The upstream is non-idempotent.** The lease prevents two drivers
  from *attempting* the dispatch at the same time, but it does not
  prevent a single driver from dispatching, crashing before recording,
  and a successor blindly retrying. The fix for that is the
  [outbox dispatch + reconciliation](non-idempotent-upstreams.md)
  pattern, which is *built on top of* leases.
- **The store loses the journal.** A corrupted store is outside Tape's
  recovery model. Run on a transactional backend (Postgres, AlloyDB,
  Spanner) for any workload where this matters, and back it up like
  any other system of record.

## Operational shape

| Knob | Default | Why you'd change it |
|---|---|---|
| Lease TTL | 30 s | Lower for faster takeover at the cost of more heartbeat traffic; raise if your reactors are slow. |
| Heartbeat interval | TTL / 3 | The standard safety factor. |
| Takeover backoff | jittered exponential, 1–10 s | Avoid thundering herd when N reactors all see an expired lease. |
| Max in-flight lease attempts | unlimited | The store CAS handles contention; the loser just retries. |

The CLI exposes these per-reactor in `tape.yaml`:

```yaml
tape:
  reactors:
    recovery:
      lease_ttl_s: 30
      heartbeat_s: 10
```

## Next

- [Reactors](concepts/reactors.md) — what each reactor does under a
  lease.
- [Replay & resume](concepts/replay.md) — the mechanics of resuming
  after a takeover.
- [Architecture](architecture.md) — where leases sit in the system.
