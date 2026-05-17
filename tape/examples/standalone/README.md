# Standalone Tape examples

Each subdirectory is a complete, runnable `tape init`-style project. The
intent is one example per pattern from the docs:

  * `hello-durable-adk/`             — the smallest durable agent. 15 lines.
  * `treasury-idempotent/`           — the treatise's treasury agent, refactored.
  * `human-approval-gate/`           — a durable suspend-until-signal.
  * `reactive-kv-coordination/`      — two agents reacting to each other via tape KV.
  * `cloud-run-alloydb/`             — a deployment-shape demo with the GCP Terraform.
  * `gke-bigtable/`                  — same shape but for GKE.

For the non-idempotent-upstream kill-test (crash before dispatch / after
dispatch / real duplicate / STUCK→human-gate), see the canonical
[`tape/examples/non_idempotent_bank/`](../non_idempotent_bank/) — it exercises
the full `@tape.outbox_tool` + connector + reconciler + compensation
choreography against a fake non-idempotent bank.

Each example ships its own `tape.yaml`, so:

```bash
cd tape/examples/standalone/hello-durable-adk
pip install -e .
tape dev
```
