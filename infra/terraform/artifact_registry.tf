resource "google_artifact_registry_repository" "spark" {
  project       = var.project_id
  location      = var.region
  repository_id = "spark"
  format        = "DOCKER"
  description   = "Spark job images"

  depends_on = [google_project_service.enabled]
}
