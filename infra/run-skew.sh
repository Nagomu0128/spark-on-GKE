#!/usr/bin/env bash
# Skew experiment (Phase 5, Design §9): generate category-skewed data, then run a
# baseline aggregate (no salt) and a salted aggregate so you can compare
# wall-clock and per-task input skew in the Spark History Server.
#
#   RUN_DATE=2026-06-02 ./run-skew.sh
#
# Use a RUN_DATE distinct from real data so the synthetic skew set is isolated.
# Prerequisite: run ./image.sh first — the image bakes jobs/, so gen_skewed.py and
# aggregate.py's --salt flag must be present in the pushed image.
# Env: PROJECT_ID (default gcloud config), REGION (default asia-northeast1).
# Generator size/skew come from jobs/gen_skewed.py defaults.
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

wait_done() { # $1 = SparkApplication name; polls until terminal state
  echo "waiting for $1 ..."
  for _ in $(seq 1 90); do
    st=$(kubectl -n spark-jobs get sparkapplication "$1" \
      -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
    case "${st}" in
      COMPLETED) echo "$1: COMPLETED"; return 0 ;;
      FAILED | SUBMISSION_FAILED | FAILING) echo "$1: ${st}" >&2; return 1 ;;
    esac
    sleep 10
  done
  echo "$1: timed out waiting for completion" >&2
  return 1
}

# 1) Generate the skewed raw slice (gen_skewed.py via the shared template).
RUN_DATE="${RUN_DATE}" ./run-job.sh gen_skewed
wait_done "gen-skewed-${RUN_DATE_NODASH}"

# 2) Baseline (skewed) then salted (balanced); distinct names -> both in the SHS.
for SALT in 1 16; do
  export SALT
  envsubst '${SALT} ${PROJECT_ID} ${REGION} ${SPARK_VERSION} ${RUN_DATE} ${RUN_DATE_NODASH}' \
    <../manifests/skew-aggregate.yaml | kubectl apply -f -
  wait_done "agg-salt${SALT}-${RUN_DATE_NODASH}"
done

cat <<EOF
Done. Compare in the Spark History Server (see docs/skew-experiment.md):
  agg-salt1-${RUN_DATE_NODASH}   baseline: the 'hot' key lands on one reducer (long tail task)
  agg-salt16-${RUN_DATE_NODASH}  salted: the shuffle is balanced across tasks
EOF
