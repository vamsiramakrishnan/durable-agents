# End-to-end: Tape server + reactors on Cloud Run, backed by Spanner.
# NOTE: the Spanner backend is EXPERIMENTAL. This example provisions correct
# infrastructure but the runtime will refuse to start unless
# TAPE_SPANNER_EXPERIMENTAL=1 is set in the server's env vars.

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = { source = "hashicorp/google", version = ">= 5.10" }
  }
}

provider "google" { project = var.project_id, region = var.region }

variable "project_id"        { type = string }
variable "region"            { type = string, default = "us-central1" }
variable "tape_server_image" { type = string }

module "artifact_registry" {
  source     = "../../modules/artifact-registry"
  project_id = var.project_id
  region     = var.region
}

module "iam" {
  source     = "../../modules/iam"
  project_id = var.project_id
}

module "store" {
  source           = "../../modules/spanner"
  project_id       = var.project_id
  region           = var.region
  processing_units = 100
}

resource "google_project_iam_member" "server_spanner_user" {
  project = var.project_id
  role    = "roles/spanner.databaseUser"
  member  = "serviceAccount:${module.iam.tape_server_sa_email}"
}

module "secrets" {
  source     = "../../modules/secret-manager"
  project_id = var.project_id
  secrets    = { TAPE_STORE_URL = module.store.tape_store_url }
}

module "tape_server" {
  source                = "../../modules/tape-cloud-run"
  project_id            = var.project_id
  region                = var.region
  image                 = var.tape_server_image
  service_account_email = module.iam.tape_server_sa_email
  store_url_secret      = "TAPE_STORE_URL"
  depends_on            = [module.secrets, google_project_iam_member.server_spanner_user]
}

output "tape_server_url" { value = module.tape_server.url }
output "experimental_warning" {
  value = "Spanner backend is EXPERIMENTAL. Set TAPE_SPANNER_EXPERIMENTAL=1 on the service before traffic."
}
