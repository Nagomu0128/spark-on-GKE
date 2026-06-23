#!/usr/bin/env bash
# Render and apply a batch job's SparkApplication for one run date (P2.6/P3).
# Usage: RUN_DATE=2026-06-01 ./run-job.sh <aggregate|validate_dq|publish>
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source ./versions.env

JOB="${1:?usage: RUN_DATE=YYYY-MM-DD ./run-job.sh <aggregate|validate_dq|publish>}"
case "${JOB}" in
  aggregate | validate_dq | publish | gen_skewed) ;;
  *) echo "ERROR: unknown job '${JOB}'" >&2; exit 1 ;;
esac

RUN_DATE="${RUN_DATE:?set RUN_DATE=YYYY-MM-DD}"
export JOB
export JOB_FILE="${JOB}.py"
export JOB_NAME="${JOB//_/-}" # k8s names cannot contain underscores
export RUN_DATE
export RUN_DATE_NODASH="${RUN_DATE//-/}"
export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
export REGION="${REGION:-asia-northeast1}"
export SPARK_VERSION

if [ -z "${PROJECT_ID}" ]; then
  echo "ERROR: PROJECT_ID is empty. Set it or run 'gcloud config set project <id>'." >&2
  exit 1
fi

envsubst '${JOB_NAME} ${JOB_FILE} ${PROJECT_ID} ${REGION} ${SPARK_VERSION} ${RUN_DATE} ${RUN_DATE_NODASH}' \
  < ../manifests/sparkjob.yaml | kubectl apply -f -

echo "Applied ${JOB_NAME}-${RUN_DATE_NODASH}. Watch:"
echo "  kubectl -n spark-jobs get sparkapplication ${JOB_NAME}-${RUN_DATE_NODASH} -w"
