variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for regional resources"
  type        = string
  default     = "asia-northeast1"
}

variable "lake_bucket" {
  description = "GCS data lake bucket name (no gs://). Empty => <project_id>-datalake."
  type        = string
  default     = ""
}

variable "cluster_name" {
  description = "GKE cluster name"
  type        = string
  default     = "spark-batch"
}

variable "system_machine_type" {
  description = "Machine type for the on-demand system pool (Airflow/Operator/Driver)"
  type        = string
  default     = "e2-standard-4"
}

variable "spark_machine_type" {
  description = "Machine type for the Spot Spark executor pool"
  type        = string
  default     = "n2-standard-8"
}

variable "spark_max_nodes" {
  description = "Max nodes for the autoscaling Spark Spot pool (min is 0)"
  type        = number
  default     = 8
}

variable "spark_namespace" {
  description = "Kubernetes namespace where Spark jobs run (for the Workload Identity binding)"
  type        = string
  default     = "spark-jobs"
}

variable "spark_ksa" {
  description = "Kubernetes service account name for Spark (for the Workload Identity binding)"
  type        = string
  default     = "spark"
}
