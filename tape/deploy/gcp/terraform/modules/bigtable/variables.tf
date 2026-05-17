variable "project_id" {
  type    = string
}
variable "region" {
  type    = string
}
variable "zone" {
  type    = string
  default = "us-central1-a"
}
variable "instance_name" {
  type    = string
  default = "tape"
}
variable "database_name" {
  type    = string
  default = "tape"
}
variable "num_nodes" {
  type    = number
  default = 1
}
