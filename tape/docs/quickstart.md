# Quickstart — Tape in 10 minutes

Tape is the **durable substrate for ADK agents**. This page gets you from
zero to a recovering, observable agent on Google Cloud. It has **three
acts**. Each act stands on its own — stop after Act 1 if local is all you
need today.

!!! tip "New to Tape?"
    If words like "outbox", "reactor", or "effect" don't have a meaning
    yet, the [**Concepts**](concepts/index.md) section is the mental
    model. You can read it in fifteen minutes and the rest of the docs
    will click. This page works either way.

## Prerequisites

- Python 3.10+
- A POSIX shell
- (Act 2 & 3) a GCP project with billing enabled, and `gcloud` set up

## Act 1 — local

```bash
pip install -e tape/sdk/python    # tape-py: the SDK + ADK adapter
pip install -e tape/cli           # tape:   the standalone CLI

tape init treasury                # scaffold a project (./treasury)
cd treasury
pip install -e .

tape dev          # tape-server + reactors + agent (sqlite by default)
tape doctor       # tick/cross diagnostic
```

In another shell, run your agent and watch it work. Then **kill the
agent** mid-tool-call. Run `tape doctor` — you'll see the run in
`status=RUNNABLE`. The recovery reactor (already running) re-drives it;
the confirmed effect is **not** re-executed. That's the contract.

[:material-school-outline: See a guided kill-resume walkthrough](start/first-crash-survival.md){ .md-button }

## Act 2 — GCP infrastructure

```bash
export GOOGLE_CLOUD_PROJECT=my-project

tape provision gcp --store alloydb --events pubsub --target cloud-run --dry-run
# Review the Terraform under ./deploy/gcp/terraform/.
# When happy:
tape provision gcp --apply

tape doctor --gcp
```

`tape provision gcp` is a thin wrapper over Terraform — the generated
`deploy/gcp/terraform/` is yours. Commit it. Edit it.

What you'll see provisioned:

- Artifact Registry for the server + reactor images.
- IAM SAs (`tape-server`, `tape-reactor`) with the right roles. See
  the [**IAM cheat sheet**](deploy/iam.md).
- Secret Manager (`TAPE_STORE_URL`).
- AlloyDB cluster + database for the journal.
- Pub/Sub topic for WAL fan-out (+ an outbox topic if enabled).
- A Cloud Monitoring dashboard with the five log-based metrics.

## Act 3 — GCP services

```bash
tape deploy gcp --target cloud-run
# Renders Cloud Run service specs under deploy/gcp/release/. Apply with
# the printed `gcloud run services replace ...` commands.

tape status                                # see the runs / effects / lag
tape logs --follow                         # stream Cloud Logging
```

What you got:

- Tape server on Cloud Run, fronted by Google ID-token auth (`tapes://...`).
- One Cloud Run service per enabled reactor (recovery, reconciler,
  outbox, timers, compensation).
- Everything's wired into Cloud Logging + Cloud Trace.

[:material-cloud-outline: Full Cloud Run topology](gcp-cloud-run.md){ .md-button }
[:material-kubernetes: GKE Autopilot path](gcp-gke.md){ .md-button }

## Where to go next

<div class="grid cards tape-card-grid" markdown>

-   :material-school-outline: **Learn the model**

    ---

    Read the concepts so the rest of the docs clicks into place.

    [:octicons-arrow-right-24: Concepts](concepts/index.md)

-   :material-toolbox-outline: **Build a real tool**

    ---

    The 15-line `durable_app(...)` recipe, with both effect patterns.

    [:octicons-arrow-right-24: ADK on Tape](adk.md)

-   :material-shield-check-outline: **Non-idempotent upstreams**

    ---

    Wires, payments, one-shot side effects. The outbox pattern.

    [:octicons-arrow-right-24: Non-idempotent upstreams](non-idempotent-upstreams.md)

-   :material-server-network: **Operate it**

    ---

    Stores, reactors, observability, tenancy — the operator's bag.

    [:octicons-arrow-right-24: Storage](stores.md) ·
    [Observability](observability.md) ·
    [Reactors](reactors.md)

-   :material-bookshelf: **Look it up**

    ---

    Every API on one page. Then the per-language reference.

    [:octicons-arrow-right-24: Cheat sheet](reference/cheatsheet.md) ·
    [SDK reference](reference/index.md)

-   :material-help-circle-outline: **Get unstuck**

    ---

    FAQ, troubleshooting, glossary, links to discussions.

    [:octicons-arrow-right-24: Help](help/index.md)

</div>
