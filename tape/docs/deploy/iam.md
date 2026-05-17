# IAM cheat sheet

Tape uses two GCP service accounts. The IAM module in
`tape/deploy/gcp/terraform/modules/iam/` creates them; this page is the
"why" so you can audit / customise / reproduce by hand.

## The two service accounts

| Service account | Used by | Created by |
|---|---|---|
| `tape-server@PROJECT.iam.gserviceaccount.com` | The `tape-server` Cloud Run service | `iam` Terraform module |
| `tape-reactor@PROJECT.iam.gserviceaccount.com` | Every `tape-reactor-*` Cloud Run service | `iam` Terraform module |

The agent runs under its own service account (Cloud Run / Agent Engine /
GKE — wherever you host it). That SA needs `roles/run.invoker` on
`tape-server`.

## Required roles

### `tape-server@…`

| Role | Why |
|---|---|
| `roles/secretmanager.secretAccessor` | Reads `TAPE_STORE_URL` from Secret Manager |
| `roles/logging.logWriter` | Cloud Logging |
| `roles/cloudtrace.agent` | Cloud Trace (OTel) |
| Per store: one of `roles/alloydb.client`, `roles/bigtable.user`, `roles/spanner.databaseUser` | Talks to the journal backend |
| (optional) `roles/pubsub.publisher` | If WAL fan-out to Pub/Sub is enabled |

### `tape-reactor@…`

| Role | Why |
|---|---|
| `roles/run.invoker` on `tape-server` | All reactors call into the server |
| `roles/secretmanager.secretAccessor` | Reads `TAPE_STORE_URL` (some reactors talk directly to the store too) |
| `roles/logging.logWriter` | Cloud Logging |
| `roles/cloudtrace.agent` | Cloud Trace |
| (per topology) `roles/pubsub.subscriber`, `roles/cloudtasks.enqueuer` | Event-driven mode |
| (for the recovery reactor's agent re-drive) the platform's invoker role: `roles/aiplatform.user` for Agent Engine, `roles/run.invoker` on the agent service for Cloud Run | Re-drives the agent |

### Caller (agent → tape-server)

| Role | Why |
|---|---|
| `roles/run.invoker` on `tape-server` | The SDK attaches a Google ID token from ADC, audience-bound to the Cloud Run URL |

That's the entire authn / authz path. No service-to-service tokens to
mint, no JWTs to sign yourself.

## Mapping to topology

### Cloud Run

The Cloud Run modules grant `roles/run.invoker` from the reactor SAs to
the server. You only need to grant the **agent's** SA `roles/run.invoker`
on `tape-server` yourself (if the agent is running outside the module).

### GKE Autopilot

Workload Identity binds Kubernetes Service Accounts to the GCP SAs:

```
serviceAccount:PROJECT.svc.id.goog[tape/tape-server]
serviceAccount:PROJECT.svc.id.goog[tape/tape-reactor]
```

You grant each KSA member `roles/iam.workloadIdentityUser` on the
matching GSA. The chart sets the
`iam.gke.io/gcp-service-account=<gsa-email>` annotation on the KSA.

## tapes:// and ID tokens

The Python SDK opens TLS to a `tapes://` host and attaches an OIDC ID
token from Application Default Credentials, **audience-bound to the
Cloud Run service URL**. The other-language SDKs do the same (Java
accepts a `Bearer` token; TS / Go auto-attach from ADC).

The receiving Cloud Run service validates the token against
`roles/run.invoker` on the caller's principal. That's the entire
identity story.

## Auditing your install

```bash
tape doctor --gcp
```

`doctor --gcp` checks:

- Service accounts exist.
- IAM bindings are in place.
- The store SA has the right per-backend role.
- The Cloud Run services have the right SAs attached.
- Secret Manager has `TAPE_STORE_URL` and the right SAs can read it.

It prints a tick/cross per check with the exact `gcloud` command to fix
each.

## Doing this by hand (no Terraform)

If you don't want to use the Terraform module, the IAM you need is small.
The full incantation in `gcloud`:

```bash
PROJECT=my-project
gcloud iam service-accounts create tape-server --project="$PROJECT"
gcloud iam service-accounts create tape-reactor --project="$PROJECT"

for ROLE in \
    roles/secretmanager.secretAccessor \
    roles/logging.logWriter \
    roles/cloudtrace.agent \
    roles/alloydb.client; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
      --member="serviceAccount:tape-server@$PROJECT.iam.gserviceaccount.com" \
      --role="$ROLE"
done

for ROLE in \
    roles/secretmanager.secretAccessor \
    roles/logging.logWriter \
    roles/cloudtrace.agent; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
      --member="serviceAccount:tape-reactor@$PROJECT.iam.gserviceaccount.com" \
      --role="$ROLE"
done

# Reactor → Server invoker
gcloud run services add-iam-policy-binding tape-server \
    --member="serviceAccount:tape-reactor@$PROJECT.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --region="$REGION"
```

## See also

- [Cloud Run deployment](../gcp-cloud-run.md) — the full topology.
- [GKE Autopilot](../gcp-gke.md) — Workload Identity specifics.
- [Tenancy](../tenancy.md) — how IAM relates to tenancy modes.
