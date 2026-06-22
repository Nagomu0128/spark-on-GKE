"""Daily batch DAG (P4.3): ingest_check -> aggregate -> validate_dq -> publish.

Each task applies a SparkApplication (the generic template spark_application.yaml)
via SparkKubernetesOperator. Design invariants:
- {{ ds }} (logical date) propagates to --run-date and the output partition (§8).
- catchup=True + max_active_runs=1 -> backfill works, no overlapping runs (§6.4).
- Tasks are idempotent (§8), so retries are safe.

Project/region/image come from env (set in the Airflow Helm values):
GCP_PROJECT_ID, GCP_REGION, SPARK_VERSION.

NOTE: SparkKubernetesOperator's API (application_file vs template_spec) and its
completion-wait behavior depend on the installed apache-airflow-providers-cncf-kubernetes
version (§6.4). Verify against the pinned provider; add SparkKubernetesSensor if the
operator does not wait for terminal state.
"""
import datetime
import os

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "REPLACE_ME")
REGION = os.environ.get("GCP_REGION", "asia-northeast1")
SPARK_VERSION = os.environ.get("SPARK_VERSION", "3.5.6")

# (task_id, job file, executor_instances). task_id doubles as the k8s name stem
# (underscores -> hyphens in the template). ingest_check / validate_dq / publish do
# little or driver-side work, so they don't need the aggregate's executor fan-out.
JOBS = [
    ("ingest_check", "ingest_check.py", 1),
    ("aggregate", "aggregate.py", 3),
    ("validate_dq", "validate_dq.py", 1),
    ("publish", "publish.py", 1),
]

default_args = {"retries": 2, "retry_delay": datetime.timedelta(minutes=5)}

with DAG(
    dag_id="kaggle_batch",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    # spark_application.yaml sits next to this file regardless of the mount path.
    template_searchpath=[os.path.dirname(os.path.abspath(__file__))],
    tags=["spark", "batch"],
) as dag:
    previous = None
    for task_id, job_file, executor_instances in JOBS:
        task = SparkKubernetesOperator(
            task_id=task_id,
            namespace="spark-jobs",
            application_file="spark_application.yaml",
            kubernetes_conn_id="kubernetes_default",
            delete_on_termination=True,  # clean up on finish so re-runs don't collide
            params={
                "job_name": task_id.replace("_", "-"),
                "job_file": job_file,
                "executor_instances": executor_instances,
                "project_id": PROJECT_ID,
                "region": REGION,
                "spark_version": SPARK_VERSION,
            },
        )
        if previous is not None:
            previous >> task
        previous = task
