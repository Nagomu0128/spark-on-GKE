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

# Ensure the Spark event-log directory exists so spark.eventLog.dir is valid on
# first run (Spark requires eventLog.dir to pre-exist; it does not create it).
#
# Use a placeholder object *under* the prefix (spark-events/.keep), NOT an object
# literally named "spark-events/". A "spark-events/" marker object collides with
# the GCS connector's mkdirs: Spark's event-log writer calls mkdirs(spark-events/),
# the connector tries to (re)create that exact marker with ifGenerationMatch=0,
# and the pre-existing marker makes it fail with HTTP 412 Precondition Failed.
# A child object makes spark-events/ an implicit directory, so mkdirs is a no-op.
resource "google_storage_bucket_object" "spark_events_prefix" {
  name    = "spark-events/.keep"
  bucket  = google_storage_bucket.lake.name
  content = " "
}
