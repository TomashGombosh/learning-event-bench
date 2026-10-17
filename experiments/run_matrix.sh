#!/usr/bin/env bash
# Executes experiments/configs/matrix.csv with Docker Compose. Resumable: finished runs are skipped.
# A stuck cell is timed out, logged, and the matrix continues. A non-zero exit at the end
# (any failed cell) is what run-bench.sh turns into an S3 upload and power-off.
# Usage: ./experiments/run_matrix.sh [matrix.csv]
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=experiments/lib/budget.sh
source "$(dirname "$0")/lib/budget.sh"
cleanup() { docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT
MATRIX=${1:-experiments/configs/matrix.csv}
DURATION_S=${DURATION_S:-240}
apply_window_defaults "$DURATION_S"
export DURATION_S
CELL_TIMEOUT_S="${CELL_TIMEOUT_S:-$(cell_timeout_seconds "$WARMUP_S" "$DURATION_S" "$DRAIN_S")}"
COMPOSE_UP_TIMEOUT_S="${COMPOSE_UP_TIMEOUT_S:-180}"
SHARDS=${SHARDS:-4}
# Unset -> default impairment. Explicit empty NETEM= disables it.
if [ -z "${NETEM+x}" ]; then
  NETEM="delay 10ms 5ms distribution normal"
fi
export NETEM
# The qdisc is used inside the loadgen netns, but only the host can load the module.
if [ -n "$NETEM" ] && command -v modprobe >/dev/null 2>&1; then
  modprobe sch_netem || echo "warning: modprobe sch_netem failed; loadgen netem will not work" >&2
fi
echo "per-cell loadgen timeout ${CELL_TIMEOUT_S}s (warmup=${WARMUP_S} duration=${DURATION_S} drain=${DRAIN_S} kill_at=${KILL_AT_S} compose_up=${COMPOSE_UP_TIMEOUT_S}s)"

# docker compose run attaches stdin. A `tail | while read` pipe is that stdin, so the
# first loadgen consumes every remaining matrix row and the stack is torn down after one cell.
failed_cells=()
while IFS=, read -r stage broker load rooms clients rate nu kill rep run_id; do
  # make_matrix used to write CRLF; a trailing CR becomes part of the directory name.
  stage=${stage%$'\r'}; broker=${broker%$'\r'}; load=${load%$'\r'}; rooms=${rooms%$'\r'}
  clients=${clients%$'\r'}; rate=${rate%$'\r'}; nu=${nu%$'\r'}; kill=${kill%$'\r'}
  rep=${rep%$'\r'}; run_id=${run_id%$'\r'}
  if [ -d "results/$run_id" ] && ls results/$run_id/shard-*/summary.json >/dev/null 2>&1; then echo "skip $run_id"; continue; fi
  echo "=== $run_id ($(date -Is))"
  export BROKER=$broker
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  mkdir -p "results/$run_id"
  if ! timeout -k 20s "$COMPOSE_UP_TIMEOUT_S" \
      docker compose up -d --no-build --wait redis kafka gateway1 gateway2 gateway3 \
      > "results/$run_id/compose-up.log" 2>&1; then
    echo "cell $run_id failed: compose up timed out or failed after ${COMPOSE_UP_TIMEOUT_S}s" | tee "results/$run_id/cell-failed.txt" >&2
    failed_cells+=("$run_id")
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
    continue
  fi
  sleep 5
  # Track the killer separately. A bare `wait` also waits for this sleeper, so a
  # finished loadgen still blocked the cell until KILL_AT_S (and a hung loadgen forever).
  kill_pid=""
  if [ "$kill" = "1" ]; then
    (sleep "$KILL_AT_S"; docker compose kill gateway2 >/dev/null; echo "killed gateway2") &
    kill_pid=$!
  fi
  pids=()
  for ((s = 0; s < SHARDS; s++)); do
    shard_log="results/${run_id}/loadgen-shard-${s}.log"
    # -T and stdin /dev/null: do not read the matrix. tee keeps the shard log and bench.log.
    # timeout is the hard cap: a stuck join ack cannot pin the instance for hours.
    (
      timeout -k 20s "$CELL_TIMEOUT_S" \
        docker compose --profile load run --rm --pull never -T \
        -e RUN_ID="$run_id" -e BROKER_LABEL="$broker" -e ROOMS="$rooms" -e CLIENTS_PER_ROOM="$clients" \
        -e RATE_PER_CLIENT="$rate" -e DISCONNECTS_PER_MIN="$nu" -e MEAN_OFFLINE_S=2 \
        -e WARMUP_S="$WARMUP_S" -e DURATION_S="$DURATION_S" -e DRAIN_S="$DRAIN_S" \
        -e SHARD_INDEX="$s" -e SHARD_COUNT="$SHARDS" -e SEED="$((1000 + rep))" \
        loadgen < /dev/null > >(tee "$shard_log") 2>&1
    ) </dev/null &
    pids+=($!)
  done
  shard_failed=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      rc=$?
      shard_failed=1
      echo "cell $run_id shard pid $pid failed (exit $rc)" | tee -a "results/$run_id/cell-failed.txt" >&2
      if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ] || [ "$rc" -eq 143 ]; then
        echo "cell $run_id hit per-cell timeout ${CELL_TIMEOUT_S}s" | tee -a "results/$run_id/cell-failed.txt" >&2
        docker compose --profile load kill loadgen >/dev/null 2>&1 || true
        docker compose --profile load rm -sf loadgen >/dev/null 2>&1 || true
      fi
    fi
  done
  if [ -n "$kill_pid" ]; then
    kill "$kill_pid" 2>/dev/null || true
    wait "$kill_pid" 2>/dev/null || true
  fi
  docker compose logs --no-color gateway1 gateway2 gateway3 > "results/$run_id/gateways.log" 2>&1 || true
  if [ "$shard_failed" -ne 0 ] || ! ls "results/$run_id"/shard-*/summary.json >/dev/null 2>&1; then
    echo "cell $run_id produced no results (shard_failed=$shard_failed)" | tee -a "results/$run_id/cell-failed.txt" >&2
    failed_cells+=("$run_id")
    continue
  fi
done < <(tail -n +2 "$MATRIX")
docker compose down -v --remove-orphans
if [ "${#failed_cells[@]}" -gt 0 ]; then
  echo "failed cells (${#failed_cells[@]}): ${failed_cells[*]}" >&2
  exit 1
fi
