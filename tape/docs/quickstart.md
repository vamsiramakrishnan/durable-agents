# Quickstart — Tape in 10 minutes

Tape is the **durable substrate for ADK agents**. This page gets you from zero
to a recovering, observable agent on Google Cloud. It has three acts.

## Act 1 — local

```bash
pip install -e tape/sdk/python    # tape-py: the SDK + ADK adapter
pip install -e tape/cli           # tape: the standalone CLI

tape init treasury
cd treasury
pip install -e .

tape dev          # tape-server + reactors + agent (sqlite by default)
tape doctor       # tick/cross diagnostic
```

In another shell, run your agent and watch it work. Kill the agent
mid-tool-call. Run `tape doctor` — you'll see the run in
`status=RUNNABLE`. The recovery reactor (already running) re-drives it; the
confirmed effect is **not** re-executed. That's the contract.

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

## Act 3 — GCP services

```bash
tape deploy gcp --target cloud-run
# Renders Cloud Run service specs under deploy/gcp/release/. Apply with the
# printed `gcloud run services replace ...` commands.

tape status                                # see the runs / effects / lag
tape logs --follow                         # stream Cloud Logging
```

## What you got

  * Tape server on Cloud Run, fronted by Google ID-token auth (`tapes://...`).
  * One Cloud Run service per enabled reactor (recovery, reconciler, outbox,
    timers, compensation).
  * AlloyDB cluster + database for the journal.
  * Pub/Sub topic for the WAL fan-out (and an outbox topic if you enabled it).
  * Secret Manager for `TAPE_STORE_URL`.
  * A Cloud Monitoring dashboard wired to log-based metrics.

## Where to go next

  * `adk.md` — the 15-line `durable_app(...)` recipe and `@tape.outbox_tool`.
  * `non-idempotent-upstreams.md` — wires, payments, anything you must not
    double-fire.
  * `gcp-cloud-run.md` — production deployment, IAM, network shape.
  * `gcp-gke.md` — the Helm chart for GKE Autopilot.
  * `stores.md` — sqlite / postgres / alloydb / spanner / bigtable trade-offs.
  * `observability.md` — logs, traces, dashboards.
  * `tenancy.md` — single vs multi-tenant deployment modes.
