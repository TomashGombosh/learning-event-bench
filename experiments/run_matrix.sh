#!/usr/bin/env bash
# Executes experiments/configs/matrix.csv with Docker Compose. Resumable: finished runs are skipped.
# Usage: ./experiments/run_matrix.sh [matrix.csv]
set -euo pipefail
cd "$(dirname "$0")/.."
MATRIX=${1:-experiments/configs/matrix.csv}
WARMUP_S=${WARMUP_S:-30}
DURATION_S=${DURATION_S:-240}
DRAIN_S=${DRAIN_S:-15}
SHARDS=${SHARDS:-4}
KILL_AT_S=${KILL_AT_S:-150}
NETEM=${NETEM:-"delay 10ms 5ms distribution normal"}
export NETEM

tail -n +2 "$MATRIX" | while IFS=, read -r stage broker load rooms clients rate nu kill rep run_id; do
  if [ -d "results/$run_id" ] && ls results/$run_id/shard-*/summary.json >/dev/null 2>&1; then echo "skip $run_id"; continue; fi
  echo "=== $run_id ($(date -Is))"
  export BROKER=$broker
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose up -d --wait redis kafka gateway1 gateway2 gateway3 >/dev/null
  sleep 5
  if [ "$kill" = "1" ]; then (sleep $((KILL_AT_S)); docker compose kill gateway2 >/dev/null; echo "killed gateway2") & fi
  pids=()
  for ((s = 0; s < SHARDS; s++)); do
    docker compose --profile load run --rm \
      -e RUN_ID="$run_id" -e BROKER_LABEL="$broker" -e ROOMS="$rooms" -e CLIENTS_PER_ROOM="$clients" \
      -e RATE_PER_CLIENT="$rate" -e DISCONNECTS_PER_MIN="$nu" -e MEAN_OFFLINE_S=2 \
      -e WARMUP_S="$WARMUP_S" -e DURATION_S="$DURATION_S" -e DRAIN_S="$DRAIN_S" \
      -e SHARD_INDEX="$s" -e SHARD_COUNT="$SHARDS" -e SEED="$((1000 + rep))" \
      loadgen > "results/.log-$run_id-$s.txt" 2>&1 &
    pids+=($!)
  done
  wait "${pids[@]}" || echo "WARNING: a shard failed in $run_id"
  wait
  docker compose logs --no-color gateway1 gateway2 gateway3 > "results/$run_id/gateways.log" 2>&1 || true
done
docker compose down -v --remove-orphans
