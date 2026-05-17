variable "project_id" {
  type    = string
}
variable "region" {
  type    = string
}
variable "image" {
  type    = string
}
variable "service_account_email" {
  type    = string
}
variable "store_url_secret" {
  type    = string
  default = "TAPE_STORE_URL"
}
variable "min_instances" {
  type    = number
  default = 0
}
variable "max_instances" {
  type    = number
  default = 10
}
variable "cpu" {
  type    = string
  default = "1"
}
variable "memory" {
  type    = string
  default = "512Mi"
}
variable "ingress" {
  type    = string
  default = "internal"
}
variable "vpc_connector" {
  type    = string
  default = ""
}
