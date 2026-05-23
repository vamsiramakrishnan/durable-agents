---
hide:
  - navigation
  - toc
---

# A durable journal underneath your ADK agent { .tape-hero-title }

Crash-survival, exactly-once-effective tool calls, and an explicit `UNKNOWN`
outcome — without changing one line of your ADK agent code.
{ .tape-hero-sub }

<div class="tape-hero-actions" markdown>
[:material-rocket-launch-outline: 10-minute quickstart](quickstart.md){ .md-button .md-button--primary }
[:material-map-outline: Concepts](concepts/index.md){ .md-button }
[:material-book-open-page-variant-outline: The treatise](design/agents-that-act-treatise.md){ .md-button }
[:material-github: GitHub](https://github.com/vamsiramakrishnan/durable-agents){ .md-button }
</div>

!!! tip "Three reading paths"
    The docs are dense — that's deliberate. To save you reading in the
    wrong order, pick the path that matches what you're doing right now:
    **[:material-rocket-launch: Beginner](paths/beginner.md)** ("I want
    to see what this is"),
    **[:material-server: Operator](paths/operator.md)** ("I have to run
    this in production"), or
    **[:material-atom: Systems](paths/systems.md)** ("I want to
    understand the model").

---

## Wire it in fifteen lines { .tape-section-h }

=== ":material-language-python: Python"

    ```python
    import tape
    from tape.adk import durable_app

    app, runner = durable_app(
        name="treasury",
        agent=root_agent,                                # your ADK LlmAgent
        budget=tape.Budget(usd_cap=50, token_cap=2_000_000),
    )
    ```

=== ":simple-go: Go"

    ```go
    import (
        "context"
        tape "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go"
    )

    d, _ := tape.NewDurableApp(ctx, tape.DurableConfig{
        Name:   "treasury",
        Budget: tape.Budget{USDCap: 50, TokenCap: 2_000_000},
    })
    defer d.Close()
    ```

=== ":material-language-typescript: TypeScript"

    ```ts
    import { durableApp } from 'tape-ts';

    const app = durableApp({
      name: 'treasury',
      budget: { usdCap: 50, tokenCap: 2_000_000 },
    });
    ```

=== ":fontawesome-brands-java: Java"

    ```java
    import dev.tape.DurableApp;

    try (DurableApp app = DurableApp.wire(new DurableApp.Config()
            .name("treasury")
            .budget(new DurableApp.Budget(50.0, 2_000_000)))) {
        // app.client() is the journalled gRPC client
    }
    ```

Tool bodies stay plain. Mark a tool with `@tape.outbox_tool(...)` and the
outbox reactor owns the dispatch — non-idempotent upstreams stop being a
footgun. → [ADK on Tape](adk.md)

---

## What you get { .tape-section-h }

<div class="grid cards tape-card-grid" markdown>

-   :material-restart:{ .lg .middle } **Crash-survival**

    ---

    Every decision and effect is journaled. A crashed run reconstructs and
    continues. Confirmed effects are not re-executed — **once is the
    contract**.

    [:octicons-arrow-right-24: Replay & resume](concepts/replay.md)

-   :material-shield-check-outline:{ .lg .middle } **Non-idempotent safety**

    ---

    Outbox + reconciliation makes `UNKNOWN` a first-class outcome.
    Blind retry on a wire transfer is **structurally impossible**.

    [:octicons-arrow-right-24: Non-idempotent upstreams](non-idempotent-upstreams.md)

-   :material-cloud-outline:{ .lg .middle } **Production on GCP**

    ---

    Cloud Run · GKE Autopilot · AlloyDB · Bigtable · Spanner · Pub/Sub ·
    Secret Manager · OTel. One Typer CLI composes the whole topology.

    [:octicons-arrow-right-24: Deploy on Cloud Run](gcp-cloud-run.md)

-   :material-translate-variant:{ .lg .middle } **Four SDKs, one wire**

    ---

    Python, Go, TypeScript, Java — identical surface, language-idiomatic
    shape. The CLI stays Python; your agent process can be any of the four.

    [:octicons-arrow-right-24: SDK reference](reference/index.md)

-   :material-target:{ .lg .middle } **Sharp primitives**

    ---

    Reactive KV with watch streams. Durable gates. Budgets as run state.
    Compensation registries. A reactor event bus.

    [:octicons-arrow-right-24: Concepts](concepts/index.md)

-   :material-eye-outline:{ .lg .middle } **Visible IaC**

    ---

    Terraform/OpenTofu modules + a Helm chart you can read, edit, and own.
    No black-box managed runtime — the substrate is yours.

    [:octicons-arrow-right-24: Cloud Run topology](gcp-cloud-run.md)

</div>

---

## Choose your entry point { .tape-section-h }

<div class="grid cards tape-card-grid" markdown>

-   :material-account-hard-hat:{ .lg .middle } **I want to build an agent**

    ---

    Start with the quickstart. Ten minutes to a recovering, observable agent
    on your laptop. Then read the ADK-on-Tape recipe.

    1. [Install](start/install.md)
    2. [Quickstart](quickstart.md)
    3. [ADK on Tape](adk.md)
    4. [First crash-survival](start/first-crash-survival.md)

-   :material-school-outline:{ .lg .middle } **I want to learn the model**

    ---

    The Concepts section is the mental model: journal, effects, reactors,
    `UNKNOWN`, compensation, replay. Read these before you scale.

    1. [Why Tape exists](concepts/why-tape.md)
    2. [The journal](concepts/journal.md)
    3. [Effects & idempotency](concepts/effects.md)
    4. [UNKNOWN — the third outcome](concepts/unknown.md)

-   :material-server-network:{ .lg .middle } **I'm going to operate this**

    ---

    Stores, reactors, observability, tenancy. Then the deployment topology
    and the IAM cheat sheet.

    1. [Storage backends](stores.md)
    2. [Observability](observability.md)
    3. [Cloud Run deployment](gcp-cloud-run.md)
    4. [IAM cheat sheet](deploy/iam.md)

-   :material-head-cog-outline:{ .lg .middle } **I want the argument**

    ---

    The treatise is the *why*. It's long; it earns the read. The spec turns
    the argument into a system; the parity matrix names the trade-offs.

    1. [Treatise](design/agents-that-act-treatise.md)
    2. [Tape spec](design/tape.md)
    3. [Event bus](design/tape-event-bus.md)
    4. [Parity matrix](design/parity.md)

</div>

---

## Three commands to production { .tape-section-h }

```bash
pip install -e tape/sdk/python    # tape-py — SDK + ADK adapter
pip install -e tape/cli           # tape   — the standalone CLI

tape init treasury                # scaffold a new project
cd treasury && tape dev           # server + reactors + agent on sqlite

tape provision gcp --apply        # render & apply Terraform on GCP
tape deploy gcp --target cloud-run
```

→ [Quickstart](quickstart.md) walks through each command with what to expect.

---

## Honest project status { .tape-section-h }

| Surface | Status |
|---|---|
| Python SDK + ADK adapter | :material-check-bold:{ .tape-good } Reference implementation |
| Go / TypeScript / Java SDKs | :material-check-bold:{ .tape-good } Wired client + smoke tests |
| Rust server | :material-check-bold:{ .tape-good } SQLite / Postgres / AlloyDB |
| Bigtable backend | :material-check-bold:{ .tape-good } Production-ready |
| Spanner backend | :material-flask-outline:{ .tape-warn } Experimental — gated |
| Hard multi-tenancy | :material-tools:{ .tape-warn } Design-only — proto change pending |
| Continue-as-new, child workflows | :material-tools:{ .tape-warn } Roadmap |

The roadmap and trade-offs are explicit on the
[parity matrix](design/parity.md). When something doesn't exist yet, the docs
say so.

---

[:material-rocket-launch-outline: Get started in 10 minutes](quickstart.md){ .md-button .md-button--primary }
[:material-help-circle-outline: Help & FAQ](help/index.md){ .md-button }
