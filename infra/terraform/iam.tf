# GSA used by Spark for GCS access via Workload Identity (no static keys).
resource "google_service_account" "spark" {
  project      = var.project_id
  account_id   = "spark-gsa"
  display_name = "Spark GCS access"

  depends_on = [google_project_service.enabled]
}

# Least privilege: object admin scoped to the lake bucket only.
resource "google_storage_bucket_iam_member" "spark_object_admin" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.spark.email}"
}

# Workload Identity: allow the KSA (spark_namespace/spark_ksa) to impersonate the GSA.
resource "google_service_account_iam_member" "spark_wi" {
  service_account_id = google_service_account.spark.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.spark_namespace}/${var.spark_ksa}]"

  # The workload identity pool (PROJECT.svc.id.goog) only exists once the GKE
  # cluster with workload_identity_config is created. Without this dependency the
  # binding can run first and fail with "Identity Pool does not exist".
  depends_on = [google_container_cluster.primary]
}
