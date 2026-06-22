#!/usr/bin/env bash
# Build & push the Spark job image to Artifact Registry (P2.1).
# Env: PROJECT_ID (default: gcloud config), REGION (default: asia-northeast1).
set -euo pipefail
cd "$(dirname "$0")/.."   # repo root (Dockerfile + jobs/ are here)
# shellcheck disable=SC1091
source infra/versions.env

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-asia-northeast1}"
if [ -z "${PROJECT_ID}" ]; then
  echo "ERROR: PROJECT_ID is empty. Set it or run 'gcloud config set project <id>'." >&2
  exit 1
fi

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/spark/spark-gcs:${SPARK_VERSION}"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

docker build \
  --build-arg "SPARK_VERSION=${SPARK_VERSION}" \
  --build-arg "GCS_CONNECTOR_VERSION=${GCS_CONNECTOR_VERSION}" \
  -t "${IMAGE}" .

docker push "${IMAGE}"
echo "Pushed ${IMAGE}"
