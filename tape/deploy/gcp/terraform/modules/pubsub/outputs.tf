output "events_topic"       { value = google_pubsub_topic.events.id }
output "outbox_topic"       { value = try(google_pubsub_topic.outbox[0].id, "") }
output "dlq_topic"          { value = try(google_pubsub_topic.dlq[0].id, "") }
output "events_subscription" { value = google_pubsub_subscription.events_reactor.id }
