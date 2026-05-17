variable "project_id" {
  type    = string
}
variable "region" {
  type    = string
}
variable "instance_name" {
  type    = string
  default = "tape"
}
variable "database_name" {
  type    = string
  default = "tape"
}
variable "tier" {
  type    = string
  default = "db-custom-2-7680"
}
variable "high_availability" {
  type    = bool
  default = false
}
