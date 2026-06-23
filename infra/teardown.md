# Teardown & cost (Phase 6 / Design §14, P6.4)

This is a learning project on fixed credit: **nothing runs permanently.** Tear
down when you stop working.

## What costs money

| Resource | Bills while | Stop by |
| :---- | :---- | :---- |
| GKE control plane | cluster exists | `terraform destroy` (cluster) |
| `system` node (1× e2-standard-4, on-demand) | cluster exists | `terraform destroy` (cluster) |
| `spark` nodes (Spot, n2-standard-8) | a job is running | autoscaling `min=0` → idle = 0 nodes (automatic) |
| GCS data lake | always (tiny for this data) | `terraform destroy` (full) |
| Artifact Registry image | always (small) | `terraform destroy` (full) |

The `spark` pool scales to **0** on its own when idle (verify below), so idle
compute cost is just the control plane + the one system node.

## Stop in-cluster apps (optional, before destroy)

In-cluster Helm/kubectl resources die with the cluster, but to stop them without
deleting the cluster:

```sh
cd infra
./history-server.sh delete                 # if running
helm uninstall airflow -n airflow          # stop Airflow
helm uninstall spark-operator -n spark-operator
```

## Verify spark pool scaled to 0 (P6.4 DoD)

```sh
kubectl get nodes -l workload=spark        # expect: No resources found (0 nodes when idle)
```

## Full teardown (stop all billing)

Removes exactly what Terraform tracks — cluster, node pools, bucket (incl. data,
which is re-uploadable), GSA/IAM, Artifact Registry:

```sh
cd infra/terraform
terraform destroy
```

Confirm nothing bills afterward:

```sh
gcloud container clusters list             # empty
gcloud storage buckets list                # lake bucket gone (full destroy)
```

## Keep the data lake, drop only the expensive cluster

Honors compute/storage separation (Design §6.1) — recreate the cluster later with
`terraform apply` + re-run the in-cluster bootstrap:

```sh
cd infra/terraform
terraform destroy \
  -target=google_container_node_pool.spark \
  -target=google_container_node_pool.system \
  -target=google_container_cluster.primary
# recreate later: terraform apply  (then ./k8s-bootstrap.sh, ./operator.sh, ...)
```

## Recreate

`terraform apply` → `./k8s-bootstrap.sh` → `./operator.sh` → (optionally
`./image.sh`, `./airflow.sh`). Because GCP resources are in Terraform state,
destroy/apply cycles leave nothing orphaned.
