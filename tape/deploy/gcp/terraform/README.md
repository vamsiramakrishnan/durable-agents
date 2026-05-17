# Tape on GCP — Terraform / OpenTofu modules

Reusable modules and worked examples for provisioning a production-grade Tape
deployment on Google Cloud. These are the modules `tape provision gcp` wires
into a generated root module — but they are also usable directly from your own
Terraform if you'd rather skip the CLI.

```
modules/
  artifact-registry/             # the Docker repo for tape-server + tape-reactor
  iam/                           # service accounts + roles for server / reactors
  secret-manager/                # secrets the server/reactors need at runtime
  pubsub/                        # tape-events / tape-outbox / tape-dlq topics
  alloydb/                       # the recommended SQL backend
  postgres-cloudsql/             # Cloud SQL Postgres (alternative)
  spanner/                       # globally-consistent backend (experimental)
  bigtable/                      # high-scale, row-oriented backend
  tape-cloud-run/                # tape-server on Cloud Run
  tape-reactors-cloud-run/       # one Cloud Run service per enabled reactor
  tape-gke/                      # tape on GKE Autopilot (Helm-based)
  observability/                 # log-based metrics + dashboard

examples/
  cloud-run-alloydb/             # end-to-end: Cloud Run + AlloyDB + Pub/Sub
  cloud-run-spanner/             # end-to-end: Cloud Run + Spanner (experimental)
  cloud-run-bigtable/            # end-to-end: Cloud Run + Bigtable
  gke-bigtable/                  # end-to-end: GKE Autopilot + Bigtable
```

Every module follows the same conventions:

  * inputs in `variables.tf`, outputs in `outputs.tf`, resources in `main.tf`;
  * the `google` and `google-beta` providers come from the root module;
  * Cloud Run services use Workload Identity (no JSON keys);
  * sensitive values live in Secret Manager and are referenced by name.
