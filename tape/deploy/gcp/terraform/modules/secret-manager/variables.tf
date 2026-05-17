variable "project_id" {
  type    = string
}
variable "region" {
  type    = string
  default = "us-central1"
}
variable "secrets" {
  type        = map(string)
  default     = {}
  description = "Map of secret-id -> secret-value. Values are sensitive — pass via a tfvars file outside source control."
  sensitive   = true
}
