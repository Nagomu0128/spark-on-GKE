locals {
  # Empty var => derive a sensible default.
  lake_bucket = var.lake_bucket != "" ? var.lake_bucket : "${var.project_id}-datalake"
  zone        = var.zone != "" ? var.zone : "${var.region}-a"

  # APIs Terraform enables (Service Usage + Resource Manager are bootstrapped manually).
  gcp_apis = [
    "compute.googleapis.com",
    "container.googleapis.com",
    "artifactregistry.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
  ]
}
