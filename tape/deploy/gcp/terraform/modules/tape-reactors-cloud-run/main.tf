locals {
  tape_url = replace(var.tape_server_url, "https://", "tapes://")
}

resource "google_cloud_run_v2_service" "reactor" {
  for_each = toset(var.reactors)

  project  = var.project_id
  location = var.region
  name     = "tape-reactor-${each.key}"
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = var.service_account_email
    scaling {
      min_instance_count = 1
      max_instance_count = 3
    }
    containers {
      image = var.image
      command = [
        "python", "-m", "tape.reactors",
        "--runner-from", var.runner_factory,
        "--url", local.tape_url,
        "--only", each.key,
      ]
      env {
        name  = "TAPE_URL"
        value = local.tape_url
      }
      env {
        name  = "TAPE_REACTOR"
        value = each.key
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = var.project_id
      }
      env {
        name  = "GOOGLE_CLOUD_LOCATION"
        value = var.region
      }
    }
  }
}

# Grant the reactor SA invoker rights on the Tape server (so tapes:// + ID
# token works end-to-end).
resource "google_cloud_run_v2_service_iam_member" "reactor_can_invoke_server" {
  project  = var.project_id
  location = var.region
  name     = "tape-server"
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.service_account_email}"
}
