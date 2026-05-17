# Observability

Structured logs + OpenTelemetry span-name constants. Importing `tape.obs` has zero side effects
and zero third-party dependencies — adapters install hooks.

```python
from tape.obs import log_json, span, SPAN_DISPATCH_EFFECT

log_json("effect.dispatched", run_id="r-1", tool="wire_money", reactor="outbox")

with span(SPAN_DISPATCH_EFFECT, run_id="r-1") as sp:
    ...  # your code; sp is None if no OTel tracer is installed
```

::: tape.obs
    options:
      heading_level: 2
      members_order: source
      show_root_heading: false
