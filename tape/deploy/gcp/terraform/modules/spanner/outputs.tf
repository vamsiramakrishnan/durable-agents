output "instance_name"  { value = google_spanner_instance.this.name }
output "database_name"  { value = google_spanner_database.tape.name }
output "tape_store_url" {
  value = "spanner://${var.project_id}/${google_spanner_instance.this.name}/${google_spanner_database.tape.name}"
}
