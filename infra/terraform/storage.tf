resource "google_storage_bucket" "lake" {
  name     = local.lake_bucket
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Learning project: let `terraform destroy` remove the bucket even with objects.
  # To keep the data lake across cost teardowns, destroy only the cluster (see
  # infra/README.md "Teardown").
  force_destroy = true

  depends_on = [google_project_service.enabled]
}

# Ensure the Spark event-log prefix exists so spark.eventLog.dir is valid on
# first run (the GCS connector treats this object as a directory marker).
resource "google_storage_bucket_object" "spark_events_prefix" {
  name    = "spark-events/"
  bucket  = google_storage_bucket.lake.name
  content = " "
}
