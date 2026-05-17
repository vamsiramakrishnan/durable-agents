# Bigtable instance + the single `tape` table with the `m` column family,
# GC policy `maxversions=1`. This eliminates the "create the table first"
# caveat from the happy path — `tape doctor --gcp` verifies it.

resource "google_bigtable_instance" "this" {
  project = var.project_id
  name    = var.instance_name

  cluster {
    cluster_id   = "${var.instance_name}-c1"
    zone         = var.zone
    num_nodes    = var.num_nodes
    storage_type = "SSD"
  }
}

resource "google_bigtable_table" "tape" {
  project       = var.project_id
  name          = var.database_name
  instance_name = google_bigtable_instance.this.name

  column_family {
    family = "m"
  }
}

# Note: the Bigtable Terraform provider doesn't expose GC policy on
# `google_bigtable_table` directly via column_family; use the dedicated
# google_bigtable_gc_policy.
resource "google_bigtable_gc_policy" "m_maxversions_1" {
  project       = var.project_id
  instance_name = google_bigtable_instance.this.name
  table         = google_bigtable_table.tape.name
  column_family = "m"
  gc_rules = jsonencode({
    rules = [{ max_version = 1 }]
  })
  deletion_policy = "ABANDON"
}
