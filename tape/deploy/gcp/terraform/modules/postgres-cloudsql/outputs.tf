output "instance_connection_name" { value = google_sql_database_instance.this.connection_name }
output "database_name"            { value = google_sql_database.tape.name }
output "username"                 { value = google_sql_user.tape.name }
output "password" {
  value     = random_password.tape.result
  sensitive = true
}
