output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.primary.name
}

output "region" {
  description = "GCP region"
  value       = var.region
}

output "lake_bucket" {
  description = "GCS data lake bucket"
  value       = google_storage_bucket.lake.name
}

output "spark_gsa_email" {
  description = "GSA email for the KSA Workload Identity annotation"
  value       = google_service_account.spark.email
}

output "artifact_registry_repo" {
  description = "Artifact Registry docker repo path"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.spark.repository_id}"
}

output "get_credentials_command" {
  description = "Run this to point kubectl at the cluster"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.primary.name} --region ${var.region} --project ${var.project_id}"
}
