# End-to-end: Tape on GKE Autopilot, backed by Bigtable. The cluster is
# provisioned here; the workloads themselves install via the bundled Helm chart
# in tape/deploy/gcp/k8s/chart/tape/:
#
#   helm upgrade --install tape ./tape/deploy/gcp/k8s/chart/tape \
#     -n tape --create-namespace \
#     -f values.yaml

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = { source = "hashicorp/google", version = ">= 5.10" }
  }
}

provider "google" { project = var.project_id, region = var.region }

variable "project_id" { type = string }
variable "region"     { type = string, default = "us-central1" }
variable "zone"       { type = string, default = "us-central1-a" }

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

resource "google_project_iam_member" "server_bigtable_user" {
  project = var.project_id
  role    = "roles/bigtable.user"
  member  = "serviceAccount:${module.iam.tape_server_sa_email}"
}

module "gke" {
  source     = "../../modules/tape-gke"
  project_id = var.project_id
  region     = var.region
  cluster    = "tape-autopilot"
}

output "cluster_name"   { value = module.gke.cluster_name }
output "tape_store_url" { value = module.store.tape_store_url }
