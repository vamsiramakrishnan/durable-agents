# Reactors

A **reactor** is a background loop that closes a specific gap between
"what the journal says happened" and "what the world says happened." Tape
ships five reactors. Each is **idempotent**. Each runs under a CAS lease.
Each is replaceable.

## The five reactors

| Reactor | Watches | Closes the gap by |
|---|---|---|
| `recovery` | runs in `RUNNABLE` / `RUNNING` with stale lease / `WAITING` signalled | re-driving via the runner factory |
| `reconciler` | effects in `UNKNOWN` (and optionally long-`PENDING`) | calling `@tape.effect(status_check=…)` or `connector.observe(...)` |
| `outbox` | effects flagged `dispatch=outbox` | calling `connector.dispatch(effect)` |
| `timers` | timers whose `fire_at_ms` has passed | firing `gate_timeout` / `redrive` / `reconcile` |
| `compensation` | `PENDING` obligations + `COMMITTED` with stale lease | calling the registered `compensate(...)` |

You enable them in `tape.yaml`:

```yaml
agent:
  runner_factory: app.agent:build_runner
tape:
  reactors:
    recovery:     {enabled: true}
    reconciler:   {enabled: true}
    outbox:       {enabled: true}
    timers:       {enabled: true}
    compensation: {enabled: true}
```

`tape dev` and `tape deploy gcp` both honour this. On Cloud Run, each
reactor becomes its own service (`tape-reactor-{name}`) with
`min-instances >= 1` so the loop keeps ticking.

## Why so many?

Because each closes a *different* gap. They could in principle be one big
loop, but separating them lets you scale, observe, and roll them
independently. If your wires are timing out a lot, you want to scale the
reconciler. If you're seeing recovery lag, you want more recovery workers.
A single loop hides those signals.

## How a reactor stays safe

Every reactor follows the same pattern:

1. **Find work** — query the server for runs / effects / timers in a
   state the reactor handles.
2. **Acquire a lease** — CAS-update the row with a `(reactor_name,
   owner_id, ttl)`. If the CAS fails, somebody else got it.
3. **Do the work** — call into the SDK / connector / runner factory.
4. **Update the state** — write the new status (and any side-effect
   records) back via the journal.
5. **Heartbeat** — periodically re-bump the lease while still working.
6. **Release** — drop the lease on success / failure / panic.

The per-run lease means you can run as many reactor replicas as you like.
Two workers can't both think they own the same run; one will lose the
CAS and move on.

## Polling vs event-driven

The default reactor mode is **polling**. It works everywhere — sqlite,
postgres, alloydb, bigtable, spanner. Polling intervals are configurable
(`tape.yaml` has them, with sensible defaults).

For high-volume environments you can swap to **event-driven** mode:

- Tape's WAL fans out to Pub/Sub via
  `tape.reactors.run_event_fanout(url, sink=PubSubSink(...))`.
- Each reactor becomes a Pub/Sub push subscription on a Cloud Run handler.
- `Cloud Tasks` becomes the timer backend (`createTask + scheduleTime`)
  instead of the polling timer reactor.

The protocol doesn't change. The reactor's *implementation* changes from
a loop to event handlers.

## Where reactors run

You have three deployment options, in increasing isolation:

- **`tape dev`** — every reactor runs in-process as a thread.
- **`tape-reactors --runner-from app.agent:build_runner`** — every
  reactor in one sidecar process. Good for VMs.
- **Cloud Run / GKE** — one service / Deployment per reactor. Standard
  prod shape.

Whichever you pick, the lease + the idempotent RPCs make multi-replica
safe. Recovery especially: you *want* multiple replicas so a crashed
reactor doesn't strand any runs.

## Writing your own reactor?

99% of the time you don't need to — the five ship-with reactors cover the
designed gaps. If you find yourself wanting one (e.g., a periodic
"reap stuck obligations older than 24h" loop), do it as a *new* reactor
that watches a state the existing ones don't:

```python
from tape.reactors import run_reactors

def reap_aged_obligations_once(client, *, age_hours=24):
    for ob in client.list_obligations(min_age_hours=age_hours, status="PENDING"):
        ...
```

Then add it to your `tape.yaml`. Reactors are not framework magic; they're
small, idempotent loops over the journal.

## Next

- [Compensation & sagas](compensation.md) — the compensation reactor in
  detail.
- [Replay & resume](replay.md) — what the recovery reactor actually
  does on re-drive.
- [How-to: configure reactors](../reactors.md) — operational knobs.
