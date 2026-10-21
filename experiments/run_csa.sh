#!/usr/bin/env bash
# CSA scheme × transport matrix. Smoke and full both fail fast on a harness fault.
# Usage: ./experiments/run_csa.sh [smoke|full]
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=${1:-smoke}
python3 experiments/make_csa_matrix.py "$MODE"
export VALIDITY_FAIL_FAST=1
if [ "$MODE" = "smoke" ]; then
  MATRIX=experiments/configs/csa-smoke.csv
else
  MATRIX=experiments/configs/csa-matrix.csv
fi
exec ./experiments/run_matrix.sh "$MATRIX"
