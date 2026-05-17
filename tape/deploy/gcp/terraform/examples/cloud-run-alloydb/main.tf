# End-to-end: Tape server + reactors on Cloud Run, backed by AlloyDB,
# with Pub/Sub for events. Requires PSA-peered VPC for AlloyDB private IP.
#
#   tofu init
#   tofu apply -var project_id=MY -var region=us-central1 \
#              -var tape_server_image=us-central1-docker.pkg.dev/MY/tape/tape-server:0.1

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google      = { source = "hashicorp/google",      version = ">= 5.10" }
    google-beta = { source = "hashicorp/google-beta", version = ">= 5.10" }
  }
}

provider "google"      { project = var.project_id, region = var.region }
provider "google-beta" { project = var.project_id, region = var.region }

variable "project_id"        { type = string }
variable "region"            { type = string, default = "us-central1" }
variable "tape_server_image" { type = string }
variable "vpc_connector"     { type = string, default = "" }

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
  source        = "../../modules/alloydb"
  project_id    = var.project_id
  region        = var.region
  instance_name = "tape"
}

module "secrets" {
  source     = "../../modules/secret-manager"
  project_id = var.project_id
  secrets    = { TAPE_STORE_URL = "postgres://tape:CHANGE_ME@127.0.0.1:5432/tape" }
}

module "events" {
  source     = "../../modules/pubsub"
  project_id = var.project_id
}

module "tape_server" {
  source                = "../../modules/tape-cloud-run"
  project_id            = var.project_id
  region                = var.region
  image                 = var.tape_server_image
  service_account_email = module.iam.tape_server_sa_email
  store_url_secret      = "TAPE_STORE_URL"
  vpc_connector         = var.vpc_connector
  depends_on            = [module.secrets]
}

module "tape_reactors" {
  source                = "../../modules/tape-reactors-cloud-run"
  project_id            = var.project_id
  region                = var.region
  tape_server_url       = module.tape_server.url
  service_account_email = module.iam.tape_reactor_sa_email
  image                 = var.tape_server_image
}

module "observability" {
  source     = "../../modules/observability"
  project_id = var.project_id
  region     = var.region
}

output "tape_server_url" { value = module.tape_server.url }
