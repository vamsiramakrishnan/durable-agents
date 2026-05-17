# AlloyDB cluster + primary instance + database. Private IP via PSA on the
# given network — caller must have configured service networking peering.

resource "google_alloydb_cluster" "this" {
  provider   = google-beta
  project    = var.project_id
  cluster_id = var.instance_name
  location   = var.region
  network_config {
    network = var.network
  }
}

resource "google_alloydb_instance" "primary" {
  provider      = google-beta
  cluster       = google_alloydb_cluster.this.name
  instance_id   = "${var.instance_name}-primary"
  instance_type = "PRIMARY"
  machine_config {
    cpu_count = var.cpu_count
  }
}

# We deliberately do NOT create the database/role here — AlloyDB doesn't expose
# them via the API at parity with Cloud SQL. The Tape server creates the schema
# at startup via the embedded migration runner; you need a DB-level role first.
# The bootstrap is a one-off `gcloud alloydb users create` you do after apply:
#
#   gcloud alloydb users create tape --cluster=<cluster> --region=<region> \
#       --type=BUILT_IN --password=$(openssl rand -hex 16)
#
# Then store the connection URL in Secret Manager via the secret-manager module.

output "cluster_id" { value = google_alloydb_cluster.this.cluster_id }
output "primary_uid" { value = google_alloydb_instance.primary.uid }
output "ip_address"  { value = google_alloydb_instance.primary.ip_address }
