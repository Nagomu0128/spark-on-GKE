resource "google_project_service" "enabled" {
  for_each = toset(local.gcp_apis)
  project  = var.project_id
  service  = each.value

  # Keep APIs enabled on destroy (other projects/resources may rely on them).
  disable_on_destroy = false
}
