resource "google_artifact_registry_repository" "tape" {
  project       = var.project_id
  location      = var.region
  repository_id = var.repository
  format        = "DOCKER"
  description   = "Tape — server + reactor container images"

  docker_config {
    immutable_tags = false
  }
}
