# Standalone Tape examples

Each subdirectory is a complete, runnable `tape init`-style project. The
intent is one example per pattern from the docs:

  * `hello-durable-adk/`             — the smallest durable agent. 15 lines.
  * `treasury-idempotent/`           — the treatise's treasury agent, refactored.
  * `non-idempotent-bank-outbox/`    — the kill-test. Proves no double wire.
  * `human-approval-gate/`           — a durable suspend-until-signal.
  * `reactive-kv-coordination/`      — two agents reacting to each other via tape KV.
  * `cloud-run-alloydb/`             — a deployment-shape demo with the GCP Terraform.
  * `gke-bigtable/`                  — same shape but for GKE.

Each example ships its own `tape.yaml`, so:

```bash
cd tape/examples/standalone/hello-durable-adk
pip install -e .
tape dev
```
