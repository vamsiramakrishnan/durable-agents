# Reactors

Five reactors ship in the box. Each is **idempotent** (the per-run lease + the
fact that every server RPC is idempotent makes a double-run harmless), so
run as many copies of each as you like behind a load balancer.

| Reactor       | Watches             | Reacts by                                  |
|---|---|---|
| `recovery`    | runs in RUNNABLE / RUNNING-with-stale-lease / WAITING-signalled | re-driving via the runner factory  |
| `reconciler`  | effects in UNKNOWN (and optionally long-PENDING)                | calling `@tape.effect(status_check=...)`     |
| `outbox`      | outbox-flagged effects                                           | calling `connector.dispatch(effect)`         |
| `timers`      | due timers                                                       | firing `gate_timeout`/`redrive`/`reconcile`  |
| `compensation`| PENDING obligations + COMMITTED-with-stale-lease                 | calling the registered `compensate()` (or `connector.compensate`) |

## Running them

In `tape.yaml`:

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

`tape dev` and `tape deploy gcp` both honour this. On Cloud Run, each reactor
becomes its own service (`tape-reactor-{name}`), `min-instances >= 1` so the
loop keeps ticking.

## Event-driven mode

Polling works everywhere; for high-volume environments, swap to the
event-driven mode:

  * Tape's WAL → Pub/Sub via `tape.reactors.run_event_fanout(url, sink=PubSubSink(...))`.
  * Each reactor becomes a Pub/Sub push subscription on a Cloud Run handler.
  * `Cloud Tasks` becomes the timer backend (createTask + scheduleTime)
    instead of the polling timer reactor.

Same protocol; the reactor's implementation just changes from a loop to event
handlers.
