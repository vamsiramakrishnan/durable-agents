---
hide:
  - navigation
  - toc
---

# Tape

**A durable-execution substrate for ADK agents.**

When an LLM is the orchestrator, an agent that *acts* needs a **journal underneath it** — recorded
decisions, idempotent effects, an explicit `unknown` outcome, action gates, budgets-as-state,
compensation, replay-as-memory. Tape is that journal: a Rust server with a language-agnostic gRPC
protocol and SDKs in **Python, Go, TypeScript, and Java** that plug into Google's
[Agent Development Kit](https://google.github.io/adk-docs/) with no changes to ADK.

[:material-rocket-launch-outline: 10-minute quickstart](quickstart.md){ .md-button .md-button--primary }
[:material-book-open-page-variant-outline: Read the treatise](design/agents-that-act-treatise.md){ .md-button }
[:material-github: vamsiramakrishnan/durable-agents](https://github.com/vamsiramakrishnan/durable-agents){ .md-button }

---

## A durable ADK agent in 15 lines

```python
import tape
from tape.adk import durable_app

app, runner = durable_app(
    name="treasury",
    agent=root_agent,
    budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
)
```

Tool bodies stay plain. Mark a tool with `@tape.outbox_tool(...)` and Tape's outbox reactor
owns the dispatch — non-idempotent upstreams stop being a footgun.

## What you get

<div class="grid cards" markdown>

-   :material-restart: **Crash-survival**
    Every decision and effect is journaled. A crashed run reconstructs and continues — once.

-   :material-shield-check-outline: **Non-idempotent contract**
    Outbox + reconciliation. UNKNOWN is a first-class outcome; blind retry is structurally
    impossible.

-   :material-cloud-outline: **Production on GCP**
    Cloud Run · GKE Autopilot · AlloyDB · Bigtable · Spanner · Pub/Sub · Secret Manager · OTel.
    One Typer CLI composes the whole topology.

-   :material-language-typescript: **All four SDKs**
    Python, Go, TypeScript, Java — identical surface, language-idiomatic shape.

-   :material-target: **Sharp primitives**
    Reactive KV, durable gates, budgets-as-state, compensation registries, reactor event bus.

-   :material-eye-outline: **Visible IaC**
    Terraform/OpenTofu modules + a Helm chart you can read, edit, and own.

</div>

## Three commands

```bash
pip install -e tape/sdk/python    # tape-py
pip install -e tape/cli           # tape

tape init treasury
cd treasury
tape dev                          # server + reactors + agent (sqlite)

tape provision gcp --apply        # render & apply Terraform
tape deploy gcp --target cloud-run
```

## Where to go next

- [**Quickstart**](quickstart.md) — get to first crash-survival in 10 minutes.
- [**ADK on Tape**](adk.md) — `durable_app`, `@tape.effect`, `@tape.outbox_tool`.
- [**Non-idempotent upstreams**](non-idempotent-upstreams.md) — the contract that owns the dispatch.
- [**Cloud Run deployment**](gcp-cloud-run.md) — the production topology.
- [**Python API**](reference/python/index.md) · [**Go**](reference/go/index.md) ·
  [**TypeScript**](reference/typescript/index.md) · [**Java**](reference/java/index.md) ·
  [**CLI**](reference/cli/index.md)
