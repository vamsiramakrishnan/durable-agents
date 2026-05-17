resource "google_pubsub_topic" "events" {
  project = var.project_id
  name    = var.topic
}

resource "google_pubsub_topic" "outbox" {
  count   = var.outbox_topic != "" ? 1 : 0
  project = var.project_id
  name    = var.outbox_topic
}

resource "google_pubsub_topic" "dlq" {
  count   = var.dlq_topic != "" ? 1 : 0
  project = var.project_id
  name    = var.dlq_topic
}

# Default subscriptions — one per reactor that wants to consume the WAL.
resource "google_pubsub_subscription" "events_reactor" {
  project = var.project_id
  name    = "${var.topic}-reactor"
  topic   = google_pubsub_topic.events.id

  ack_deadline_seconds = 60
  retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }
  dead_letter_policy {
    dead_letter_topic     = length(google_pubsub_topic.dlq) > 0 ? google_pubsub_topic.dlq[0].id : null
    max_delivery_attempts = 10
  }
}
