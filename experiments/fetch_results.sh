#!/usr/bin/env bash
# Download a run from S3 and analyse it locally. Usage: RESULTS_BUCKET=... ./experiments/fetch_results.sh [run_id]
# With BASE_RUN_ID set, run_id is a profile=cells rerun: the base run is downloaded first and
# every cell the rerun contains (same run id) replaces the base cell before the analysis.
# The base matrix is kept, and results/merged-from.json records which cells came from where.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${RESULTS_BUCKET:?set RESULTS_BUCKET}"
RUN_ID=${1:-}
BASE_RUN_ID=${BASE_RUN_ID:-}
if [ -z "$RUN_ID" ]; then
  RUN_ID=$(aws s3 ls "s3://${RESULTS_BUCKET}/runs/" | awk '{print $2}' | sed 's#/##' | sort | tail -n 1)
fi
echo "run: $RUN_ID"
aws s3 cp "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/DONE.json" - 2>/dev/null || echo "WARNING: DONE.json missing (run still in progress or failed)"
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
if [ -n "$BASE_RUN_ID" ]; then
  echo "base run: $BASE_RUN_ID"
  aws s3 sync "s3://${RESULTS_BUCKET}/runs/${BASE_RUN_ID}/results" results --only-show-errors
  rm -rf results-rerun
  aws s3 sync "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/results" results-rerun --only-show-errors
  $PY experiments/merge_rerun.py results results-rerun "$BASE_RUN_ID" "$RUN_ID"
else
  aws s3 sync "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/results" results --only-show-errors
fi
MATRIX=results/matrix.csv; [ -f "$MATRIX" ] || MATRIX=experiments/configs/matrix.csv
$PY analysis/analyze.py results "$MATRIX"
