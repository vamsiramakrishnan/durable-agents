# Reactors

Five reactors ship: `recovery`, `reconciler`, `outbox`, `timers`, `compensation`. Each is
**idempotent** — the per-run lease + the fact that every server RPC is idempotent makes a
double-run harmless. Scale freely.

```bash
tape-reactors --runner-from app.agent:build_runner --url tape://localhost:7878
```

::: tape.reactors.recover_once
    options:
      heading_level: 3
::: tape.reactors.reconcile_once
    options:
      heading_level: 3
::: tape.reactors.fire_due_timers_once
    options:
      heading_level: 3
::: tape.reactors.run_reactors
    options:
      heading_level: 3
::: tape.reactors.run_event_fanout
    options:
      heading_level: 3
::: tape.reactors.run_outbox_relay
    options:
      heading_level: 3

## Outbox reactor

::: tape.reactors.outbox
    options:
      heading_level: 3
      members_order: source
      show_root_heading: false
