resource "google_cloud_run_v2_service" "tape_server" {
  project  = var.project_id
  location = var.region
  name     = "tape-server"
  ingress  = var.ingress == "internal" ? "INGRESS_TRAFFIC_INTERNAL_ONLY" : "INGRESS_TRAFFIC_ALL"

  template {
    service_account = var.service_account_email
    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }
    containers {
      name  = "tape-server"
      image = var.image
      ports {
        name           = "h2c"
        container_port = 7878
      }
      env {
        name  = "TAPE_LISTEN"
        value = "0.0.0.0:7878"
      }
      env {
        name = "TAPE_STORE"
        value_source {
          secret_key_ref {
            secret  = var.store_url_secret
            version = "latest"
          }
        }
      }
      env {
        name  = "RUST_LOG"
        value = "tape_server=info"
      }
      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
      }
    }
    dynamic "vpc_access" {
      for_each = var.vpc_connector != "" ? [1] : []
      content {
        connector = var.vpc_connector
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }
  }
}
