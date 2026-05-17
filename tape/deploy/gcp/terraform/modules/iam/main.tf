resource "google_service_account" "tape_server" {
  project      = var.project_id
  account_id   = "${var.service_account_prefix}-server"
  display_name = "Tape server"
}

resource "google_service_account" "tape_reactor" {
  project      = var.project_id
  account_id   = "${var.service_account_prefix}-reactor"
  display_name = "Tape reactors"
}

# Cloud Logging + Cloud Trace — both SAs.
resource "google_project_iam_member" "server_log" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.tape_server.email}"
}
resource "google_project_iam_member" "reactor_log" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.tape_reactor.email}"
}
resource "google_project_iam_member" "server_trace" {
  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.tape_server.email}"
}
resource "google_project_iam_member" "reactor_trace" {
  project = var.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.tape_reactor.email}"
}

# Secret Manager access — the server reads TAPE_STORE_URL from a secret.
resource "google_project_iam_member" "server_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.tape_server.email}"
}
resource "google_project_iam_member" "reactor_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.tape_reactor.email}"
}
