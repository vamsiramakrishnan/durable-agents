variable "project_id" {
  type    = string
}
variable "region" {
  type    = string
}
variable "tape_server_url" {
  type    = string
}
variable "service_account_email" {
  type    = string
}
variable "image" {
  type    = string
  default = ""
}
variable "runner_factory" {
  type    = string
  default = "app.agent:build_runner"
}
variable "reactors" {
  type    = list(string)
  default = ["recovery", "reconciler", "outbox", "timers", "compensation"]
}
