#!/usr/bin/env bash
# Reference deploy for Tape on GCP. Substitute PROJECT / REGION / image refs / the
# AlloyDB connection name / the Agent Engine resource name.  Usage: ./deploy.sh {server|reactor|iam}
set -euo pipefail
: "${PROJECT:?set PROJECT}"; : "${REGION:=us-central1}"
REPO="$REGION-docker.pkg.dev/$PROJECT/tape"
HERE="$(cd "$(dirname "$0")" && pwd)"
TAPE_ROOT="$HERE/../.."          # the tape/ directory

case "${1:-}" in

server)
  # build & push the Rust server image (build context = tape/, so ../proto is available to build.rs)
  gcloud builds submit "$TAPE_ROOT" --tag "$REPO/tape-server:0.1" --gcs-source-staging-dir="gs://$PROJECT-cloudbuild/src" \
    --pack image="$REPO/tape-server:0.1" 2>/dev/null \
  || docker build -f "$TAPE_ROOT/server/Dockerfile" -t "$REPO/tape-server:0.1" "$TAPE_ROOT" && docker push "$REPO/tape-server:0.1"
  # deploy to Cloud Run (AlloyDB path; for Bigtable: drop --add-volume/--container alloydb-proxy and set TAPE_STORE=bigtable://...)
  gcloud run deploy tape-server --region="$REGION" --image="$REPO/tape-server:0.1" \
    --use-http2 --ingress=internal --no-allow-unauthenticated \
    --min-instances=1 --max-instances=20 --cpu=1 --memory=512Mi --port=7878 \
    --service-account="tape-server@$PROJECT.iam.gserviceaccount.com" \
    --set-env-vars="TAPE_LISTEN=0.0.0.0:7878,TAPE_STORE=postgres://tape:PASSWORD@127.0.0.1:5432/tape,RUST_LOG=tape_server=info" \
    --vpc-connector="CONNECTOR" --vpc-egress=private-ranges-only
    # ...and add the AlloyDB Auth Proxy as a sidecar — easiest via the YAML:
    #   gcloud run services replace "$HERE/server.service.yaml" --region="$REGION"
  echo "Tape server URL:"; gcloud run services describe tape-server --region="$REGION" --format='value(status.url)'
  ;;

reactor)
  # build & push the reactor image (it bundles your agent package — edit reactor/Dockerfile)
  docker build -t "$REPO/tape-reactor:0.1" "$HERE/reactor" && docker push "$REPO/tape-reactor:0.1"
  TAPE_URL="$(gcloud run services describe tape-server --region="$REGION" --format='value(status.url)' | sed 's|https://|tapes://|')"
  gcloud run deploy tape-reactor --region="$REGION" --image="$REPO/tape-reactor:0.1" \
    --no-allow-unauthenticated --min-instances=1 --max-instances=3 --cpu=1 --memory=512Mi \
    --service-account="tape-reactor@$PROJECT.iam.gserviceaccount.com" \
    --set-env-vars="TAPE_URL=$TAPE_URL,AGENT_ENGINE=projects/$PROJECT/locations/$REGION/reasoningEngines/RESOURCE_ID,GOOGLE_CLOUD_PROJECT=$PROJECT,GOOGLE_CLOUD_LOCATION=$REGION"
  ;;

iam)
  # the agent's SA (Agent Engine) and the reactor's SA both need to call the Tape server
  for SA in "service-PROJECT_NUMBER@gcp-sa-aiplatform-re.iam.gserviceaccount.com" "tape-reactor@$PROJECT.iam.gserviceaccount.com"; do
    gcloud run services add-iam-policy-binding tape-server --region="$REGION" \
      --member="serviceAccount:$SA" --role="roles/run.invoker"
  done
  # the reactor calls the Agent Engine :streamQuery API
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:tape-reactor@$PROJECT.iam.gserviceaccount.com" --role="roles/aiplatform.user"
  # (Bigtable path) the Tape server's SA needs Bigtable user
  # gcloud projects add-iam-policy-binding "$PROJECT" \
  #   --member="serviceAccount:tape-server@$PROJECT.iam.gserviceaccount.com" --role="roles/bigtable.user"
  ;;

*) echo "usage: PROJECT=... REGION=... ./deploy.sh {server|reactor|iam}"; exit 2 ;;
esac
