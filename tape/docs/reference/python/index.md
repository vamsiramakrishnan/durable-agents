# Python — `tape-py`

The reference SDK. Install:

```bash
pip install -e tape/sdk/python
```

The public surface is small and curated through `tape.__init__`:

```python
import tape
from tape.adk import durable_app, TapePlugin, TapeSessionService
from tape.connectors.http import HTTPConnector

app, runner = durable_app(
    name="treasury",
    agent=root_agent,
    budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
)
```

All API pages below are generated from the SDK's own Google-style docstrings via
[`mkdocstrings`](https://mkdocstrings.github.io/) — code is the source of truth.

- [**`durable_app`**](durable.md) — wire an ADK app into Tape in one call.
- [**`@tape.effect` and `@tape.outbox_tool`**](effect.md) — annotate tool bodies.
- [**Connectors**](connectors.md) — capability connectors (HTTP, Pub/Sub).
- [**Reactors**](reactors.md) — recovery, reconciler, outbox, timers, compensation.
- [**Observability**](obs.md) — structured logs + OTel span names.
- [**Tenancy**](tenancy.md) — single / trusted-multi-app / hard-multi-tenant.
- [**Client**](client.md) — the gRPC client and re-exported status enums.
