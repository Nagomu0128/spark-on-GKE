#!/usr/bin/env bash
# Spark History Server on demand (Phase 6, Design §11). Run after jobs have
# produced event logs in gs://<lake>/spark-events/.
#
#   ./history-server.sh           # apply + how to open it
#   ./history-server.sh delete    # remove it (do not leave running -> cost)
#
# Env: PROJECT_ID (default gcloud config), REGION (default asia-northeast1).
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./versions.env

export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
export REGION="${REGION:-asia-northeast1}"
export SPARK_VERSION
if [ -z "${PROJECT_ID}" ]; then
  echo "ERROR: PROJECT_ID is empty. Set it or run 'gcloud config set project <id>'." >&2
  exit 1
fi

render() {
  envsubst '${PROJECT_ID} ${REGION} ${SPARK_VERSION}' <../manifests/history-server.yaml
}

if [ "${1:-up}" = "delete" ]; then
  render | kubectl delete -f -
  exit 0
fi

render | kubectl apply -f -
kubectl -n spark-jobs rollout status deploy/spark-history-server --timeout=120s
cat <<EOF
History Server is up. Open it with a port-forward:
  kubectl -n spark-jobs port-forward svc/spark-history-server 18080:18080
  # then browse http://localhost:18080
Stop it when done (saves cost):
  ./history-server.sh delete
EOF
