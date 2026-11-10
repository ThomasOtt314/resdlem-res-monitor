#!/usr/bin/env bash
# deploy.sh -- deploy the REVAP reservoir monitoring dashboard as a PUBLIC Cloud Function.
#
# Self-contained: everything the function needs is in this folder.
#   main.py           the function (entry point: dashboard)
#   requirements.txt  its Python packages
#   reservoirs.csv    reservoir list with OWEN_ID, LAT, LON
#   deploy.sh         this script
#
# Usage:  bash deploy.sh
#
# Pages:  <URL>/          dashboard
#         <URL>/?refresh=1 re-check the bucket now (otherwise hourly)
#         <URL>/inspect   bucket inspection report
#
# Optional settings (add to --set-env-vars below):
#   RES_PREFIX=folder   per-reservoir folder, if auto-detection picks the wrong one
#   STALE_DAYS=14  PLOT_DAYS=60  SPARK_DAYS=365  CACHE_TTL=3600
set -euo pipefail

PROJECT=nasa-revap
REGION=us-central1                       # same region as revap-update
NAME=revap-dashboard
HERE="$(cd "$(dirname "$0")" && pwd)"

# 1. Check every file the function needs is here before deploying.
for f in main.py requirements.txt reservoirs.csv; do
  if [[ ! -f "$HERE/$f" ]]; then
    echo "Missing $f in $HERE"
    exit 1
  fi
done
if ! head -1 "$HERE/reservoirs.csv" | grep -q 'LAT'; then
  echo "reservoirs.csv has no LAT column -- use the version with OWEN_ID, LAT, LON"
  exit 1
fi
echo "reservoirs.csv: $(($(wc -l < "$HERE/reservoirs.csv") - 1)) reservoirs"

# 2. Deploy, public. Entry point changed from 'inspect' to 'dashboard'.
gcloud functions deploy "$NAME" \
  --project "$PROJECT" --region "$REGION" \
  --gen2 --runtime python314 \
  --source "$HERE" --entry-point dashboard \
  --trigger-http --allow-unauthenticated \
  --memory 1Gi --timeout 300s \
  --set-env-vars BUCKET=operevap-res-retrieval-elevation

# 3. Print the public URL.
URL=$(gcloud functions describe "$NAME" --project "$PROJECT" --region "$REGION" \
      --gen2 --format='value(serviceConfig.uri)')
echo
echo "Deployed (public): $URL"
echo "Dashboard: $URL/    Inspection: $URL/inspect"
