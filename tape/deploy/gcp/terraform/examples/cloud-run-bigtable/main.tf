# End-to-end: Tape server + reactors on Cloud Run, backed by Bigtable.
# No AlloyDB Auth Proxy, no VPC connector needed — Bigtable uses IAM directly.

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = { source = "hashicorp/google", version = ">= 5.10" }
  }
}

provider "google" { project = var.project_id, region = var.region }

variable "project_id"        { type = string }
variable "region"            { type = string, default = "us-central1" }
variable "zone"              { type = string, default = "us-central1-a" }
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
  source        = "../../modules/bigtable"
  project_id    = var.project_id
  region        = var.region
  zone          = var.zone
  instance_name = "tape"
  database_name = "tape"
}

# Bigtable needs roles/bigtable.user on the server SA.
resource "google_project_iam_member" "server_bigtable_user" {
  project = var.project_id
  role    = "roles/bigtable.user"
  member  = "serviceAccount:${module.iam.tape_server_sa_email}"
}

module "secrets" {
  source     = "../../modules/secret-manager"
  project_id = var.project_id
  secrets = {
    TAPE_STORE_URL = module.store.tape_store_url
  }
}

module "tape_server" {
  source                = "../../modules/tape-cloud-run"
  project_id            = var.project_id
  region                = var.region
  image                 = var.tape_server_image
  service_account_email = module.iam.tape_server_sa_email
  store_url_secret      = "TAPE_STORE_URL"
  depends_on            = [module.secrets, google_project_iam_member.server_bigtable_user]
}

module "tape_reactors" {
  source                = "../../modules/tape-reactors-cloud-run"
  project_id            = var.project_id
  region                = var.region
  tape_server_url       = module.tape_server.url
  service_account_email = module.iam.tape_reactor_sa_email
  image                 = var.tape_server_image
}

output "tape_server_url" { value = module.tape_server.url }
output "tape_store_url"  { value = module.store.tape_store_url }
