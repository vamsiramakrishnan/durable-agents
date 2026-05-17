resource "google_sql_database_instance" "this" {
  project          = var.project_id
  name             = var.instance_name
  region           = var.region
  database_version = "POSTGRES_15"
  deletion_protection = false
  settings {
    tier              = var.tier
    availability_type = var.high_availability ? "REGIONAL" : "ZONAL"
    ip_configuration {
      ipv4_enabled = true
    }
  }
}

resource "google_sql_database" "tape" {
  project  = var.project_id
  name     = var.database_name
  instance = google_sql_database_instance.this.name
}

resource "random_password" "tape" {
  length  = 24
  special = true
}

resource "google_sql_user" "tape" {
  project  = var.project_id
  name     = "tape"
  instance = google_sql_database_instance.this.name
  password = random_password.tape.result
}
