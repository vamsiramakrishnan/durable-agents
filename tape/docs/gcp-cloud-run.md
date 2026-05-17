# Tape on Cloud Run

The default production topology. Tape server, each reactor, and (optionally)
the agent itself are independent Cloud Run services. The journal lives in
AlloyDB (recommended) or Bigtable. The WAL fans out via Pub/Sub.

```
   ┌──────────────────────────┐    tapes:// + Google ID token    ┌────────────────────────┐
   │  ADK agent               │ ─────────────────────────────▶ │  tape-server           │
   │  (Cloud Run / Agent      │                                  │  Cloud Run, --use-http2 │
   │   Engine)                │ ◀───────────────────────────── │  internal ingress       │
   └──────────────────────────┘                                  └──────────┬─────────────┘
                                                                            │
                                                              AlloyDB Auth  │
                                                              Proxy sidecar │   ┌─► AlloyDB
                                                                            └──►│   or Bigtable IAM
   ┌────────────────────────────────────────────┐
   │  tape-reactor-{recovery,reconciler,outbox, │  poll / Pub/Sub push
   │   timers,compensation}  — Cloud Run        │  → re-drive via runner factory
   └────────────────────────────────────────────┘
```

## Provision

```bash
tape provision gcp --store alloydb --events pubsub --target cloud-run --apply
```

Provisions: Artifact Registry, IAM SAs (`tape-server`, `tape-reactor`),
Secret Manager (TAPE_STORE_URL), AlloyDB cluster + instance, Pub/Sub
(events + outbox + DLQ), and the Cloud Monitoring dashboard + log-based
metrics. Inspect the Terraform under `deploy/gcp/terraform/` before applying
in shared environments.

## Deploy

```bash
tape deploy gcp --target cloud-run --image-tag 0.2
```

Builds (`gcloud builds submit` or `docker build`) and pushes the
`tape-server` and `tape-reactor` images to Artifact Registry, then renders
Cloud Run service specs under `deploy/gcp/release/`. Apply via the printed
`gcloud run services replace` commands (or pipe them into your CI).

## IAM cheat sheet

  * `tape-server@PROJECT.iam.gserviceaccount.com`
    needs: `roles/secretmanager.secretAccessor` (for TAPE_STORE_URL),
    `roles/logging.logWriter`, `roles/cloudtrace.agent`. Per backend:
    `roles/alloydb.client` or `roles/bigtable.user` or `roles/spanner.databaseUser`.
  * `tape-reactor@PROJECT.iam.gserviceaccount.com`
    needs: `roles/run.invoker` on `tape-server`, the SDK clients it uses
    (`roles/pubsub.subscriber`, `roles/cloudtasks.enqueuer`),
    `roles/secretmanager.secretAccessor`, and the agent platform's role
    (`roles/aiplatform.user` if you redrive Agent Engine).

The IAM module sets the universal ones; the Cloud Run modules grant
`roles/run.invoker` from reactors to the server.

## `tapes://` and ID tokens

The Python SDK opens TLS to a `tapes://` host and attaches an OIDC ID token
from Application Default Credentials, audience-bound to the Cloud Run service
URL. Your caller's service account needs `roles/run.invoker`. That's it.
