#!/usr/bin/env bash
# Install Apache Airflow via Helm (P4.1). Run after the cluster + operator are up.
# Edit infra/values-airflow.yaml (GCP_PROJECT_ID) first.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./versions.env

helm repo add apache-airflow https://airflow.apache.org
helm repo update

# RBAC so Airflow task pods can manage SparkApplications in spark-jobs.
kubectl apply -f ../manifests/airflow-spark-rbac.yaml

helm upgrade --install airflow apache-airflow/airflow \
  --version "${AIRFLOW_CHART_VERSION}" \
  --namespace airflow --create-namespace \
  -f values-airflow.yaml \
  --wait --timeout 10m

echo "Airflow installed (chart ${AIRFLOW_CHART_VERSION}). Open the UI:"
echo "  kubectl -n airflow port-forward svc/airflow-webserver 8080:8080"
