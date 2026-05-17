# Spanner instance + database for Tape. The Tape server's Spanner backend is
# EXPERIMENTAL — `tape doctor` warns on this, and the server refuses runtime
# unless TAPE_SPANNER_EXPERIMENTAL=1 is set. The infra is correct; the runtime
# is incomplete.

resource "google_spanner_instance" "this" {
  project          = var.project_id
  name             = var.instance_name
  config           = "regional-${var.region}"
  display_name     = "tape"
  processing_units = var.processing_units
}

resource "google_spanner_database" "tape" {
  project  = var.project_id
  instance = google_spanner_instance.this.name
  name     = var.database_name

  # The Tape server applies its own DDL on startup; we leave the database empty.
  # If/when the Spanner backend graduates, the migration runner will materialize
  # the schema here.
  ddl                      = []
  deletion_protection      = false
  enable_drop_protection   = false
}
