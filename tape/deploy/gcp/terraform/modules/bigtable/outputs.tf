output "instance_name" { value = google_bigtable_instance.this.name }
output "table_name"    { value = google_bigtable_table.tape.name }
output "tape_store_url" {
  value = "bigtable://${var.project_id}/${google_bigtable_instance.this.name}/${google_bigtable_table.tape.name}"
}
