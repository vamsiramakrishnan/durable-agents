# Reference: provision a GKE Autopilot cluster suitable for the bundled Tape
# Helm chart. The chart itself lives in tape/deploy/gcp/k8s/chart/tape and is
# installed out-of-band (`helm upgrade --install ...`).

resource "google_container_cluster" "tape" {
  project          = var.project_id
  name             = var.cluster
  location         = var.region
  enable_autopilot = true

  release_channel {
    channel = "REGULAR"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}
