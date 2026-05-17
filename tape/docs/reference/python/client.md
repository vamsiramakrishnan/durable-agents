# Client

The thin gRPC client over the `tape.v1` service. Every mutating call is idempotent on the server,
so retrying is always safe.

```python
from tape import TapeClient

with TapeClient("tape://localhost:7878") as c:
    run = c.begin_run(app_name="treasury", user_id="cfo", session_id="s1",
                      invocation_id="inv-1", lease_owner="me")
```

::: tape.client.TapeClient
    options:
      heading_level: 2
      members_order: source
      filters: ["!^_"]

## Status enums

::: tape.client
    options:
      heading_level: 3
      show_root_heading: false
      members:
        - DEFAULT_URL
        - RUN_STATUS_RUNNABLE
        - RUN_STATUS_RUNNING
        - RUN_STATUS_WAITING
        - RUN_STATUS_TERMINAL
        - RUN_STATUS_FAILED
        - RUN_STATUS_STUCK
        - RUN_STATUS_CANCELLED
        - EFFECT_STATUS_PENDING
        - EFFECT_STATUS_CONFIRMED
        - EFFECT_STATUS_FAILED
        - EFFECT_STATUS_UNKNOWN
        - EFFECT_SEMANTICS_IDEMPOTENT
        - EFFECT_SEMANTICS_NON_IDEMPOTENT
        - EFFECT_SEMANTICS_OBSERVE_ONLY
        - EFFECT_DISPATCH_MODE_INLINE
        - EFFECT_DISPATCH_MODE_OUTBOX
        - EFFECT_RESOLUTION_CONFIRMED
        - EFFECT_RESOLUTION_FAILED
        - EFFECT_RESOLUTION_ABSENT
        - EFFECT_RESOLUTION_DUPLICATE
        - EFFECT_RESOLUTION_STUCK
