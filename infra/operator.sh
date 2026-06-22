#!/usr/bin/env bash
# Install the Kubeflow Spark Operator via Helm (P1.11). Run after k8s-bootstrap.sh.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./versions.env

helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm upgrade --install spark-operator spark-operator/spark-operator \
  --version "${SPARK_OPERATOR_CHART_VERSION}" \
  --namespace spark-operator --create-namespace \
  --set "spark.jobNamespaces={spark-jobs}" \
  --set webhook.enable=true \
  --wait

echo "Spark Operator installed (chart ${SPARK_OPERATOR_CHART_VERSION}). Smoke test:"
echo "  kubectl apply -f ../manifests/spark-pi.yaml"
echo "  kubectl -n spark-jobs get sparkapplication spark-pi -w"
