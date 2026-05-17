# `durable_app`

The wiring entrypoint — pass an ADK agent, get back a configured `(App, Runner)` pair.

```python
import tape
from tape.adk import durable_app

app, runner = durable_app(
    name="treasury",
    agent=root_agent,
    budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
)
```

::: tape.adk.durable.durable_app
    options:
      heading_level: 2

---

## Related

- The `(App, Runner)` pair is what `runner.run_async(...)` consumes; see ADK's
  [Runner docs](https://google.github.io/adk-docs/) for the call shape.
- For non-idempotent tools, prefer [`@tape.outbox_tool`](effect.md#tape.effect.outbox_tool).
