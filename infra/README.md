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

## Phase 1 — provision and connect

GCP resources (Terraform):

```sh
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # edit project_id etc.
terraform init
terraform plan
terraform apply
```

In-cluster bootstrap (from `infra/`):

```sh
# PROJECT_ID/REGION/CLUSTER default to gcloud config / asia-northeast1 / spark-batch
./k8s-bootstrap.sh      # credentials, namespaces, KSA(+WI annotation), RBAC
./operator.sh           # Kubeflow Spark Operator via Helm

# Smoke test (P1.12)
kubectl apply -f ../manifests/spark-pi.yaml
kubectl -n spark-jobs get sparkapplication spark-pi -w   # expect COMPLETED
```

## Phase 2 — image & first transform

```sh
cd infra
./image.sh                                   # build & push spark-gcs image to Artifact Registry

# ingest a day's slice (sample fixture provided)
RUN_DATE=2026-06-01 ./ingest.sh ../tests/fixtures/events_sample.csv

# run the aggregate job (writes staging/agg_by_category/run_date=<ds>/)
RUN_DATE=2026-06-01 ./run-job.sh aggregate
kubectl -n spark-jobs get sparkapplication aggregate-20260601 -w   # expect COMPLETED

# verify output
gcloud storage ls "gs://$(gcloud config get-value project)-datalake/staging/agg_by_category/"
```

## Phase 3 — DQ gate, publish, idempotency

```sh
cd infra
RUN_DATE=2026-06-01 ./run-job.sh validate_dq   # fails (blocks publish) if checks breach
RUN_DATE=2026-06-01 ./run-job.sh publish       # staging -> curated + _SUCCESS

# only partitions with _SUCCESS are safe to read (promotion isn't atomic on GCS)
gcloud storage ls "gs://$(gcloud config get-value project)-datalake/curated/agg_by_category/run_date=2026-06-01/"
```

Idempotency check (P3.3): run `aggregate` + `publish` for the same `RUN_DATE`
twice and confirm the curated row count/content is unchanged.

Unit-test the jobs locally (needs Java 17 + pyspark):

```sh
pip install -r tests/requirements-dev.txt
pytest tests/
```

## Phase 4 — orchestration (Airflow)

```sh
cd infra
# edit values-airflow.yaml: set GCP_PROJECT_ID (and region) so the DAG can render
./airflow.sh
kubectl -n airflow port-forward svc/airflow-webserver 8080:8080   # UI at localhost:8080
```

The `kaggle_batch` DAG (git-synced from `dags/`) chains
`ingest_check -> aggregate -> validate_dq -> publish`, propagating `{{ ds }}` to
`--run-date`. `catchup=True` + `max_active_runs=1` give backfill without overlap.

- Backfill a range (P4.4): `airflow dags backfill kaggle_batch -s 2026-06-01 -e 2026-06-03`.
- Idempotency: re-run the same logical date; curated output stays unique.

> The `SparkKubernetesOperator` API (`application_file` vs `template_spec`) and its
> completion-wait behavior depend on the installed
> `apache-airflow-providers-cncf-kubernetes` version — verify and adjust (add
> `SparkKubernetesSensor` if the operator does not wait). The RBAC binding in
> `manifests/airflow-spark-rbac.yaml` assumes the task SA is `airflow-worker`.

## Phase 5 — skew reproduction + mitigation

```sh
cd infra
./image.sh                          # rebuild image (bakes jobs/) if jobs changed
RUN_DATE=2026-06-02 ./run-skew.sh   # gen skewed data -> baseline vs salted aggregate
```

Compare `agg-salt1-*` (skewed: one long-tail task) vs `agg-salt16-*` (salted:
balanced) in the History Server. Salting (`aggregate.py --salt N`) is
result-preserving (unit-tested). Details: `docs/skew-experiment.md`.

## Phase 6 — observability & cost

```sh
cd infra
./history-server.sh                 # on-demand Spark History Server
kubectl -n spark-jobs port-forward svc/spark-history-server 18080:18080
./history-server.sh delete          # stop when done
```

Stages → Tasks shows per-task skew; Cloud Logging holds driver/executor logs.
See `docs/observability.md`.

## Teardown — stop billing (P6.4)

Nothing runs permanently. Stop in-cluster apps, confirm the spark pool scaled to
0, and `terraform destroy` (full or cluster-only). Full runbook:
**`infra/teardown.md`**.
