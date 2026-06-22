#!/usr/bin/env bash
# Upload a local CSV to raw/events/dt=<RUN_DATE>/ (P2.3).
# Usage: RUN_DATE=2026-06-01 ./ingest.sh path/to/events.csv
set -euo pipefail

RUN_DATE="${RUN_DATE:?set RUN_DATE=YYYY-MM-DD}"
SRC="${1:?usage: RUN_DATE=YYYY-MM-DD ./ingest.sh <local.csv>}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
LAKE="${LAKE:-gs://${PROJECT_ID}-datalake}"

dest="${LAKE}/raw/events/dt=${RUN_DATE}/"
gcloud storage cp "${SRC}" "${dest}"
echo "Uploaded ${SRC} -> ${dest}"
