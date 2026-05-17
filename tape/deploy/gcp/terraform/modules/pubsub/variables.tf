variable "project_id" {
  type    = string
}
variable "topic" {
  type    = string
  default = "tape-events"
}
variable "outbox_topic" {
  type    = string
  default = "tape-outbox"
}
variable "dlq_topic" {
  type    = string
  default = "tape-dlq"
}
