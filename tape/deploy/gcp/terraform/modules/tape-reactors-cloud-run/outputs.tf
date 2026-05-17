output "reactor_urls" {
  value = { for k, s in google_cloud_run_v2_service.reactor : k => s.uri }
}
