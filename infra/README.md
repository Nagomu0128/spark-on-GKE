# Infrastructure

GCP resources are managed with **Terraform** (`infra/terraform/`). In-cluster
components use **Helm** (`infra/operator.sh`, `infra/airflow.sh`) and **kubectl**
(`manifests/`). See `docs/tasks.md` for the phased plan and `docs/Design Doc.md`
for rationale.

## Prerequisites — one-time bootstrap (P0.1 / P0.2)

Manual, because it precedes project/API setup and cannot live in Terraform:

1. Create or select a GCP project and link billing.
2. Enable the Resource Manager and Service Usage APIs so Terraform can enable the rest
   (`google_project_service` needs Service Usage):
   ```sh
   gcloud services enable cloudresourcemanager.googleapis.com serviceusage.googleapis.com --project <PROJECT_ID>
   ```
3. Set a **budget alert** (Billing → Budgets & alerts). This is a learning project
   on a fixed credit — guard against runaway cost.
4. Install tools and verify: `gcloud`, `kubectl`, `helm`, `docker`, `gh`, `terraform`
   (each with `--version`).
5. Authenticate:
   ```sh
   gcloud auth login
   gcloud auth application-default login   # ADC — used by Terraform and local tools
   gcloud config set project <PROJECT_ID>
   ```

## Pinned versions

Component versions live in [`versions.env`](versions.env). Verify the latest
before bumping.

## Terraform (Phase 1)

```sh
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # edit project_id etc.
terraform init
terraform plan
terraform apply
```

## Teardown — stop billing (P6.4)

```sh
cd infra/terraform
terraform destroy
```

`terraform destroy` removes exactly what was created (state-tracked), so no
billable resources are left behind. In-cluster Helm releases are destroyed with
the cluster; run `helm uninstall` first if you want explicit cleanup.
