#!/usr/bin/env bash
# Cluster-level bootstrap (P1.8-P1.10): credentials, namespaces, KSA (+WI
# annotation), RBAC. Run after `terraform apply`.
#
# Env (override as needed):
#   PROJECT_ID  defaults to `gcloud config get-value project`
#   REGION      defaults to asia-northeast1
#   CLUSTER     defaults to spark-batch
set -euo pipefail
cd "$(dirname "$0")"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-asia-northeast1}"
CLUSTER="${CLUSTER:-spark-batch}"

if [ -z "${PROJECT_ID}" ]; then
  echo "ERROR: PROJECT_ID is empty. Set it or run 'gcloud config set project <id>'." >&2
  exit 1
fi

gcloud container clusters get-credentials "${CLUSTER}" --region "${REGION}" --project "${PROJECT_ID}"

kubectl apply -f ../manifests/namespaces.yaml
PROJECT_ID="${PROJECT_ID}" envsubst '${PROJECT_ID}' < ../manifests/ksa.yaml | kubectl apply -f -
kubectl apply -f ../manifests/spark-rbac.yaml

echo "Bootstrap complete: KSA spark-jobs/spark -> spark-gsa@${PROJECT_ID}.iam.gserviceaccount.com"
