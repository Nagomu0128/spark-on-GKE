terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0" # verify latest before bumping (see infra/versions.env)
    }
  }

  # Optional: store state in GCS for cross-machine reproducibility.
  # Create the bucket once (manually), then uncomment and `terraform init -migrate-state`.
  # backend "gcs" {
  #   bucket = "<your-tfstate-bucket>"
  #   prefix = "spark-on-k8s/state"
  # }
}
