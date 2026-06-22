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
