# Deploying {{ name }} to GCP — Tape overlay

`tape provision gcp --dry-run` writes Terraform under `deploy/gcp/terraform/`.
`tape deploy gcp --target cloud-run` renders Cloud Run service specs under
`deploy/gcp/release/`. Commit both.

See `tape/docs/gcp-cloud-run.md` in the Tape repo for the full walkthrough.
