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
variable "processing_units" {
  type    = number
  default = 100
}
