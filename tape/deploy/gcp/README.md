# Tape on Google Cloud — Agent Engine + Cloud Run + AlloyDB/Bigtable

The shape (see `design-principles/tape.md` §12 and the conversation that built
this folder):

```
   ┌──────────────────────────┐        gRPC/TLS + ID token        ┌───────────────────────────┐
   │  ADK agent               │ ────────────────────────────────▶ │  Tape server (Cloud Run)  │
   │  on Vertex AI Agent      │   TapePlugin · TapeSessionService  │  Rust · --use-http2       │
   │  Engine (or Cloud Run)   │ ◀──────────────────────────────── │  internal ingress         │
   └──────────────────────────┘                                   └──────────┬────────────────┘
            ▲                                                                 │  AlloyDB Auth Proxy
            │ :streamQuery (resume by invocation_id)                          │  sidecar  ──▶  AlloyDB
   ┌────────┴───────────────────┐   poll ListRunsToRecover /                  │      or  TAPE_STORE=bigtable://…
   │  Reactors (Cloud Run svc   │   ListPendingEffects / ListDueTimers   ──────┘
   │  or Pub/Sub-push handlers) │   — recovery · reconciler · timers
   └────────────────────────────┘
```

Three deployables:

1. **Tape server** — the Rust binary, on **Cloud Run** (`server.service.yaml`):
   stateless, autoscaled, `min-instances: 1` (it's on the model/tool hot path),
   `--use-http2` (gRPC), **internal ingress**, an **AlloyDB Auth Proxy** sidecar
   container (so `TAPE_STORE=postgres://tape:…@127.0.0.1:5432/tape`) — or drop
   the sidecar and set `TAPE_STORE=bigtable://PROJECT/INSTANCE/TABLE` (Bigtable
   uses the service account's IAM; create the table first:
   `cbt -project P -instance I createtable tape && … createfamily tape m && … setgcpolicy tape m maxversions=1`).
   Cloud Run terminates TLS at the load balancer and forwards h2c to the
   container, so the Rust server stays plaintext gRPC; clients connect over TLS.

2. **The ADK agent** — deployed to **Vertex AI Agent Engine** with
   `TapePlugin` + `TapeSessionService` + `ResumabilityConfig(is_resumable=True)`,
   `tape-py` in `requirements`, and `TAPE_URL=tapes://<tape-server-url>` in
   `env_vars` (`agent_engine_deploy.py`). The `tapes://` scheme makes the SDK
   open a TLS channel and attach a Google ID token (Application Default
   Credentials) for the Cloud Run audience — so the agent's service account just
   needs `roles/run.invoker` on the Tape server. (If you'd rather run the agent
   on Cloud Run too, it's a normal `gcloud run deploy` of your ADK app — and then
   the reactor can be a *sidecar container* in that service, same image, entrypoint
   `tape-reactors --runner-from my_app:build_runner`.)

3. **The reactors** — a small **Cloud Run service** (`reactor/`): it bundles your
   agent package (for the `@tape.effect(status_check=…)` registrations the
   reconciler needs) and runs `tape.reactors.run_reactors(...)`. Because the agent
   is on Agent Engine, its "re-drive" is a callback that calls the Agent Engine
   `:streamQuery` API rather than `runner.run_async` — `reactor/main.py` shows
   this (`run_reactors(redrive_fn=agent_engine_redrive, url=TAPE_URL)`). Each
   reactor is idempotent (the lease + replay properties make a double-run
   harmless), so scale it freely. IAM: `roles/run.invoker` on the Tape server,
   `roles/aiplatform.user` (to call `:streamQuery`), and — if you go event-driven
   — `roles/pubsub.subscriber` + `roles/cloudtasks.enqueuer`.

> **Event-driven instead of polling.** Point Tape's WAL at Pub/Sub —
> `tape.reactors.run_event_fanout(url, sink=<a Pub/Sub publisher>)` on the SQL
> stores, or **Bigtable change streams → Dataflow `BigtableChangeStreamsToPubSub`**
> on Bigtable — and turn each reactor into a Pub/Sub *push* subscription on a
> Cloud Run handler; use **Cloud Tasks** (`createTask` with `scheduleTime`) as the
> timer backend instead of the `tape_timers` table. Same `tape.proto`; the
> reactor's implementation just changes from a loop to event handlers. Cloud
> Tasks isn't required — the `tape_timers` table + the polling timer reactor
> works everywhere; Cloud Tasks is the fully-managed swap (no poller, scale to
> zero, exact-time delivery).

## Steps

```bash
export PROJECT=your-project REGION=us-central1
gcloud config set project $PROJECT

# 0. (Bigtable path) create the table once
#    cbt -project $PROJECT -instance tape createtable tape
#    cbt -project $PROJECT -instance tape createfamily tape m
#    cbt -project $PROJECT -instance tape setgcpolicy tape m maxversions=1
# 0. (AlloyDB path) create a cluster/instance + a `tape` db & role (see deploy.sh)

# 1. build & push the Tape server image; deploy to Cloud Run
./deploy.sh server      # builds tape/server, gcloud run deploy with --use-http2, internal ingress, the AlloyDB proxy sidecar

# 2. deploy the ADK agent to Agent Engine (wires TapePlugin / TapeSessionService)
python agent_engine_deploy.py

# 3. build & push the reactor image; deploy to Cloud Run; grant IAM
./deploy.sh reactor

# (optional) wire the IAM bindings
./deploy.sh iam
```

Everything here is reference — substitute your project, region, image refs, the
AlloyDB connection name, and the Agent Engine resource name. The point is the
topology and the wiring, not the exact `gcloud` invocations.
