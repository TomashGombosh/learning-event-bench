#!/usr/bin/env bash
# Executes experiments/configs/matrix.csv with Docker Compose. Resumable: finished runs are skipped.
# Usage: ./experiments/run_matrix.sh [matrix.csv]
set -euo pipefail
cd "$(dirname "$0")/.."
cleanup() { docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT
MATRIX=${1:-experiments/configs/matrix.csv}
WARMUP_S=${WARMUP_S:-30}
DURATION_S=${DURATION_S:-240}
DRAIN_S=${DRAIN_S:-15}
SHARDS=${SHARDS:-4}
KILL_AT_S=${KILL_AT_S:-150}
# Unset -> default impairment. Explicit empty NETEM= disables it.
if [ -z "${NETEM+x}" ]; then
  NETEM="delay 10ms 5ms distribution normal"
fi
export NETEM
# The qdisc is used inside the loadgen netns, but only the host can load the module.
if [ -n "$NETEM" ] && command -v modprobe >/dev/null 2>&1; then
  modprobe sch_netem || echo "warning: modprobe sch_netem failed; loadgen netem will not work" >&2
fi

# docker compose run attaches stdin. A `tail | while read` pipe is that stdin, so the
# first loadgen consumes every remaining matrix row and the stack is torn down after one cell.
while IFS=, read -r stage broker load rooms clients rate nu kill rep run_id; do
  # make_matrix used to write CRLF; a trailing CR becomes part of the directory name.
  stage=${stage%$'\r'}; broker=${broker%$'\r'}; load=${load%$'\r'}; rooms=${rooms%$'\r'}
  clients=${clients%$'\r'}; rate=${rate%$'\r'}; nu=${nu%$'\r'}; kill=${kill%$'\r'}
  rep=${rep%$'\r'}; run_id=${run_id%$'\r'}
  if [ -d "results/$run_id" ] && ls results/$run_id/shard-*/summary.json >/dev/null 2>&1; then echo "skip $run_id"; continue; fi
  echo "=== $run_id ($(date -Is))"
  export BROKER=$broker
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose up -d --no-build --wait redis kafka gateway1 gateway2 gateway3 >/dev/null
  sleep 5
  if [ "$kill" = "1" ]; then (sleep $((KILL_AT_S)); docker compose kill gateway2 >/dev/null; echo "killed gateway2") & fi
  mkdir -p "results/$run_id"
  pids=()
  for ((s = 0; s < SHARDS; s++)); do
    shard_log="results/${run_id}/loadgen-shard-${s}.log"
    # -T and stdin /dev/null: do not read the matrix. tee keeps the shard log and bench.log.
    (
      docker compose --profile load run --rm --pull never -T \
        -e RUN_ID="$run_id" -e BROKER_LABEL="$broker" -e ROOMS="$rooms" -e CLIENTS_PER_ROOM="$clients" \
        -e RATE_PER_CLIENT="$rate" -e DISCONNECTS_PER_MIN="$nu" -e MEAN_OFFLINE_S=2 \
        -e WARMUP_S="$WARMUP_S" -e DURATION_S="$DURATION_S" -e DRAIN_S="$DRAIN_S" \
        -e SHARD_INDEX="$s" -e SHARD_COUNT="$SHARDS" -e SEED="$((1000 + rep))" \
        loadgen < /dev/null 2>&1 | tee "$shard_log"
    ) </dev/null &
    pids+=($!)
  done
  shard_failed=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      shard_failed=1
    fi
  done
  wait || true
  docker compose logs --no-color gateway1 gateway2 gateway3 > "results/$run_id/gateways.log" 2>&1 || true
  if [ "$shard_failed" -ne 0 ] || ! ls "results/$run_id"/shard-*/summary.json >/dev/null 2>&1; then
    echo "cell $run_id produced no results (shard_failed=$shard_failed)" >&2
    exit 1
  fi
done < <(tail -n +2 "$MATRIX")
docker compose down -v --remove-orphans
