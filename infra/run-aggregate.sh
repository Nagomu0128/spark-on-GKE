#!/usr/bin/env bash
# Render and apply the aggregate SparkApplication for one run date (P2.6).
# Usage: RUN_DATE=2026-06-01 ./run-aggregate.sh
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./versions.env

RUN_DATE="${RUN_DATE:?set RUN_DATE=YYYY-MM-DD}"
export RUN_DATE
export RUN_DATE_NODASH="${RUN_DATE//-/}"
export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
export REGION="${REGION:-asia-northeast1}"
export SPARK_VERSION

if [ -z "${PROJECT_ID}" ]; then
  echo "ERROR: PROJECT_ID is empty. Set it or run 'gcloud config set project <id>'." >&2
  exit 1
fi

envsubst '${PROJECT_ID} ${REGION} ${SPARK_VERSION} ${RUN_DATE} ${RUN_DATE_NODASH}' \
  < ../manifests/aggregate.yaml | kubectl apply -f -

echo "Applied kaggle-agg-${RUN_DATE_NODASH}. Watch:"
echo "  kubectl -n spark-jobs get sparkapplication kaggle-agg-${RUN_DATE_NODASH} -w"
