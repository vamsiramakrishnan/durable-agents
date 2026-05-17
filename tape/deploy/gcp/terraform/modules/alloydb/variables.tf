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
variable "cpu_count" {
  type    = number
  default = 2
}
variable "network" {
  type        = string
  default     = "projects/PROJECT/global/networks/default"
  description = "Fully-qualified VPC network for private-IP AlloyDB connectivity."
}
