resource "google_container_cluster" "primary" {
  name     = var.cluster_name
  project  = var.project_id
  location = var.region # regional control plane (HA)

  release_channel {
    channel = "REGULAR"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Manage node pools as separate resources.
  remove_default_node_pool = true
  initial_node_count       = 1

  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {}

  # Regional control plane for HA, but pin nodes to a single zone. Without this,
  # the temporary default node pool spawns one node per zone (3 in
  # asia-northeast1); 3 x 100GB pd-balanced boot disks exceed the 250GB
  # SSD_TOTAL_GB quota and cluster creation fails.
  node_locations = [local.zone]

  # Learning project — allow `terraform destroy` to remove the cluster.
  deletion_protection = false

  depends_on = [google_project_service.enabled]
}

# On-demand system pool: Airflow, Operator, and Spark drivers (stable).
# Pinned to a single zone to avoid one node per zone in a regional cluster.
resource "google_container_node_pool" "system" {
  name           = "system"
  project        = var.project_id
  location       = var.region
  cluster        = google_container_cluster.primary.name
  node_locations = [local.zone]
  node_count     = 1

  node_config {
    machine_type = var.system_machine_type
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    # pd-standard boot disk keeps nodes off the tight SSD_TOTAL_GB quota
    # (pd-ssd/pd-balanced count against it; default is 250GB in asia-northeast1).
    disk_type    = "pd-standard"
    disk_size_gb = 50

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

# Spot pool for Spark executors: scales 0..N, tainted/labeled so only executors land here.
resource "google_container_node_pool" "spark" {
  name           = "spark"
  project        = var.project_id
  location       = var.region
  cluster        = google_container_cluster.primary.name
  node_locations = [local.zone]

  autoscaling {
    min_node_count = 0
    max_node_count = var.spark_max_nodes
  }

  node_config {
    machine_type = var.spark_machine_type
    spot         = true
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    # pd-standard boot disk so the pool can scale to several Spot nodes without
    # exhausting the small SSD_TOTAL_GB quota (each node gets its own boot disk).
    disk_type    = "pd-standard"
    disk_size_gb = 100

    labels = {
      workload = "spark"
    }

    taint {
      key    = "workload"
      value  = "spark"
      effect = "NO_SCHEDULE"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}
