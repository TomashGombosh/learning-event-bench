#!/usr/bin/env bash
# Download a run from S3 and analyse it locally. Usage: RESULTS_BUCKET=... ./experiments/fetch_results.sh [run_id]
set -euo pipefail
cd "$(dirname "$0")/.."
: "${RESULTS_BUCKET:?set RESULTS_BUCKET}"
RUN_ID=${1:-}
if [ -z "$RUN_ID" ]; then
  RUN_ID=$(aws s3 ls "s3://${RESULTS_BUCKET}/runs/" | awk '{print $2}' | sed 's#/##' | sort | tail -n 1)
fi
echo "run: $RUN_ID"
aws s3 cp "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/DONE.json" - 2>/dev/null || echo "WARNING: DONE.json missing (run still in progress or failed)"
aws s3 sync "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/results" results --only-show-errors
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
MATRIX=results/matrix.csv; [ -f "$MATRIX" ] || MATRIX=experiments/configs/matrix.csv
$PY analysis/analyze.py results "$MATRIX"
