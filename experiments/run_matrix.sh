#!/usr/bin/env bash
# Executes experiments/configs/matrix.csv with Docker Compose. Resumable: finished runs are skipped.
# A stuck cell is timed out, logged, and the matrix continues. A non-zero exit at the end
# (any failed cell, or fewer rows executed than the matrix has) is what run-bench.sh turns
# into an S3 upload and power-off.
# Usage: ./experiments/run_matrix.sh [matrix.csv]
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=experiments/lib/budget.sh
source "$(dirname "$0")/lib/budget.sh"
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
  modprobe sch_netem </dev/null || echo "warning: modprobe sch_netem failed; loadgen netem will not work" >&2
fi
echo "per-cell loadgen timeout ${CELL_TIMEOUT_S}s (warmup=${WARMUP_S} duration=${DURATION_S} drain=${DRAIN_S} kill_at=${KILL_AT_S} compose_up=${COMPOSE_UP_TIMEOUT_S}s)"

# A finished cell has one summary per shard that owned a room, and no failure file.
# Empty shards (rooms < SHARDS) exit 0 without a file; those are not required.
cell_complete() {
  local run_id=$1 rooms=$2
  if [ -f "results/${run_id}/cell-failed.txt" ]; then
    return 1
  fi
  local expected=$SHARDS
  if [ "$rooms" -lt "$expected" ]; then
    expected=$rooms
  fi
  if [ "$expected" -le 0 ]; then
    return 1
  fi
  local found=0
  local f
  for f in results/${run_id}/shard-*/summary.json; do
    [ -f "$f" ] || continue
    found=$((found + 1))
  done
  [ "$found" -ge "$expected" ]
}

# One grep-able line per cell. Offered is the matrix load, not the sum of whichever
# shard files happened to land: a timed-out shard used to shrink offered (3300, 800).
# SATURATED when any shard says so, achieved < 95% of nominal, or a shard file is missing.
emit_cell_summary() {
  python3 - "$1" "$2" "$3" "$4" "$5" "$6" <<'PY' || echo "CELL $1 broker=$2 offered/achieved=$3/0.0 FAILED SATURATED"
import json, sys
from pathlib import Path
run_id, broker, nominal_s, shards_s, rooms_s, failed_s = sys.argv[1:7]
nominal = float(nominal_s)
expected = min(int(rooms_s), int(shards_s))
paths = sorted(Path("results", run_id).glob("shard-*/summary.json"))
flags = []
if failed_s == "1" or not paths:
    flags.append("FAILED")
if not paths:
    flags.append("SATURATED")
    flags.append(f"shards=0/{expected}")
    print(
        f"CELL {run_id} broker={broker} offered/achieved={nominal:.1f}/0.0 "
        f"p50=0.0 p99=0.0 rooms_diverged=0 clients_lost=0 clients_not_converged=0 "
        + " ".join(flags)
    )
    raise SystemExit(0)
summaries = [json.loads(path.read_text()) for path in paths]
achieved = sum(float(s["load"]["achievedSendPerSec"]) for s in summaries)
p50 = max(float(s["latency"]["p50Ms"] or 0) for s in summaries)
p99 = max(float(s["latency"]["p99Ms"] or 0) for s in summaries)
rooms_diverged = sum(int(s.get("divergedRooms", 0)) for s in summaries)
clients_lost = sum(int(s.get("clientsLost", 0)) for s in summaries)
clients_not_converged = sum(int(s.get("clientsNotConverged", 0)) for s in summaries)
missing = len(paths) < expected
short = nominal > 0 and achieved < 0.95 * nominal
saturated = any(bool(s.get("load", {}).get("saturated")) for s in summaries) or missing or short
if saturated and "SATURATED" not in flags:
    flags.append("SATURATED")
if missing:
    flags.append(f"shards={len(paths)}/{expected}")
suffix = (" " + " ".join(flags)) if flags else ""
print(
    f"CELL {run_id} broker={broker} offered/achieved={nominal:.1f}/{achieved:.1f} "
    f"p50={p50:.1f} p99={p99:.1f} rooms_diverged={rooms_diverged} "
    f"clients_lost={clients_lost} clients_not_converged={clients_not_converged}{suffix}"
)
PY
}

# 0 = topic is gone, 1 = still listed, 2 = kafka-topics itself failed.
topic_gone() {
  local listing rc=0
  listing=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list </dev/null 2>/dev/null) || rc=$?
  if [ "$rc" -ne 0 ]; then
    return 2
  fi
  if printf '%s\n' "$listing" | grep -qx collab-events; then
    return 1
  fi
  return 0
}

# Redis and Kafka stay up across cells. Room ids are ${RUN_ID}-rN, so a new cell does
# not read the previous cell's records, but the topic and streams are wiped so catch-up
# cannot scan an ever-growing log. Gateways call createTopics on boot, so a delete while
# they are still running never sticks: the name stays in `kafka-topics --list` and the
# old 10s poll rebuilt the whole broker. Kill the gateways first, then delete, and only
# recreate Redis/Kafka if the topic is still listed after that. Returns 0 if reuse worked.
brokers_ready=0
prepare_brokers() {
  if [ "$brokers_ready" -ne 1 ]; then
    docker compose down -v --remove-orphans </dev/null >/dev/null 2>&1 || true
    return 1
  fi
  docker compose kill gateway1 gateway2 gateway3 </dev/null >/dev/null 2>&1 || true
  if ! docker compose exec -T redis redis-cli FLUSHALL </dev/null >/dev/null 2>&1; then
    echo "redis FLUSHALL failed; recreating the stack" >&2
    brokers_ready=0
    docker compose down -v --remove-orphans </dev/null >/dev/null 2>&1 || true
    return 1
  fi
  local attempt i status
  for attempt in 1 2 3; do
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --delete --topic collab-events \
      </dev/null >/dev/null 2>&1 || true
    for i in $(seq 1 20); do
      status=0
      topic_gone || status=$?
      if [ "$status" -eq 0 ]; then
        return 0
      fi
      if [ "$status" -eq 2 ]; then
        echo "kafka-topics --list failed; recreating the stack" >&2
        brokers_ready=0
        docker compose down -v --remove-orphans </dev/null >/dev/null 2>&1 || true
        return 1
      fi
      sleep 0.25
    done
  done
  echo "topic collab-events still present after delete; recreating the stack" >&2
  brokers_ready=0
  docker compose down -v --remove-orphans </dev/null >/dev/null 2>&1 || true
  return 1
}

# docker compose run/exec attach stdin. The matrix used to be that stdin
# (`while read; do ...; done < <(tail matrix)`), so the first `compose exec`
# (broker reset between cells) consumed every remaining row and the script
# exited 0 after two cells. The CSV is on fd 3, and every compose call also
# has its own </dev/null>.
executed=0
failed_cells=()
expected_cells=0
report_cells() {
  echo "CELLS_SUMMARY executed=${executed}/expected=${expected_cells} failed=${#failed_cells[@]}"
  mkdir -p results
  printf '{"executed":%s,"expected":%s,"failed":%s}\n' \
    "$executed" "$expected_cells" "${#failed_cells[@]}" > results/cells-summary.json
}
cleanup() {
  local rc=$?
  report_cells || true
  docker compose down -v --remove-orphans </dev/null >/dev/null 2>&1 || true
  if [ "$rc" -eq 0 ]; then
    if [ "$executed" -lt "$expected_cells" ]; then
      echo "matrix stopped early: executed ${executed} of ${expected_cells} rows" >&2
      exit 1
    fi
    if [ "${#failed_cells[@]}" -gt 0 ]; then
      echo "failed cells (${#failed_cells[@]}): ${failed_cells[*]}" >&2
      exit 1
    fi
  fi
}
trap cleanup EXIT
expected_cells=$(tail -n +2 "$MATRIX" | grep -c . || true)

while IFS=, read -r stage broker load rooms clients rate nu kill rep run_id <&3; do
  # make_matrix used to write CRLF; a trailing CR becomes part of the directory name.
  stage=${stage%$'\r'}; broker=${broker%$'\r'}; load=${load%$'\r'}; rooms=${rooms%$'\r'}
  clients=${clients%$'\r'}; rate=${rate%$'\r'}; nu=${nu%$'\r'}; kill=${kill%$'\r'}
  rep=${rep%$'\r'}; run_id=${run_id%$'\r'}
  if [ -z "$run_id" ]; then
    continue
  fi
  executed=$((executed + 1))
  if cell_complete "$run_id" "$rooms"; then echo "skip $run_id"; continue; fi
  echo "=== $run_id ($(date -Is))"
  export BROKER=$broker
  rm -rf "results/$run_id"
  mkdir -p "results/$run_id"
  if prepare_brokers; then
    if ! docker compose up -d --no-build --force-recreate --no-deps \
        gateway1 gateway2 gateway3 </dev/null > "results/$run_id/compose-up.log" 2>&1; then
      echo "cell $run_id failed: gateway recreate failed" | tee "results/$run_id/cell-failed.txt" >&2
      brokers_ready=0
      docker compose down -v --remove-orphans </dev/null >/dev/null 2>&1 || true
      failed_cells+=("$run_id")
      continue
    fi
  elif ! timeout -k 20s "$COMPOSE_UP_TIMEOUT_S" \
      docker compose up -d --no-build --wait redis kafka gateway1 gateway2 gateway3 \
      </dev/null > "results/$run_id/compose-up.log" 2>&1; then
    echo "cell $run_id failed: compose up timed out or failed after ${COMPOSE_UP_TIMEOUT_S}s" | tee "results/$run_id/cell-failed.txt" >&2
    brokers_ready=0
    docker compose down -v --remove-orphans </dev/null >/dev/null 2>&1 || true
    failed_cells+=("$run_id")
    continue
  else
    brokers_ready=1
  fi
  sleep 2
  # Track the killer separately. A bare `wait` also waits for this sleeper, so a
  # finished loadgen still blocked the cell until KILL_AT_S (and a hung loadgen forever).
  kill_pid=""
  if [ "$kill" = "1" ]; then
    (sleep "$KILL_AT_S"; docker compose kill gateway2 </dev/null >/dev/null; echo "killed gateway2") </dev/null 3<&- &
    kill_pid=$!
  fi
  pids=()
  shard_logs=()
  for ((s = 0; s < SHARDS; s++)); do
    shard_log="results/${run_id}/loadgen-shard-${s}.log"
    shard_logs+=("$shard_log")
    # -T and stdin /dev/null: do not read the matrix. tee keeps the shard log and bench.log.
    # timeout is the hard cap: a stuck join ack cannot pin the instance for hours.
    # 3<&- so the shard cannot hold the matrix fd open.
    (
      timeout -k 20s "$CELL_TIMEOUT_S" \
        docker compose --profile load run --rm --pull never -T \
        -e RUN_ID="$run_id" -e BROKER_LABEL="$broker" -e ROOMS="$rooms" -e CLIENTS_PER_ROOM="$clients" \
        -e RATE_PER_CLIENT="$rate" -e DISCONNECTS_PER_MIN="$nu" -e MEAN_OFFLINE_S=2 \
        -e WARMUP_S="$WARMUP_S" -e DURATION_S="$DURATION_S" -e DRAIN_S="$DRAIN_S" \
        -e SHARD_INDEX="$s" -e SHARD_COUNT="$SHARDS" -e SEED="$((1000 + rep))" \
        loadgen </dev/null > >(tee "$shard_log") 2>&1
    ) </dev/null 3<&- &
    pids+=($!)
  done
  shard_failed=0
  for i in "${!pids[@]}"; do
    pid=${pids[$i]}
    # `if ! wait` makes $? inside the then-body 0, so every timeout was logged as exit 0
    # and the 124/137 branch never ran. Capture the status before testing it.
    rc=0
    wait "$pid" || rc=$?
    if [ "$rc" -ne 0 ]; then
      shard_failed=1
      shard_log=${shard_logs[$i]}
      echo "cell $run_id shard pid $pid failed (exit $rc)" | tee -a "results/$run_id/cell-failed.txt" >&2
      echo "--- last lines of ${shard_log} ---" >&2
      tail -n 40 "$shard_log" >&2 || true
      if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ] || [ "$rc" -eq 143 ]; then
        echo "cell $run_id hit per-cell timeout ${CELL_TIMEOUT_S}s" | tee -a "results/$run_id/cell-failed.txt" >&2
        docker compose --profile load kill loadgen </dev/null >/dev/null 2>&1 || true
        docker compose --profile load rm -sf loadgen </dev/null >/dev/null 2>&1 || true
      fi
    fi
  done
  if [ -n "$kill_pid" ]; then
    kill "$kill_pid" 2>/dev/null || true
    wait "$kill_pid" 2>/dev/null || true
  fi
  docker compose logs --no-color gateway1 gateway2 gateway3 </dev/null > "results/$run_id/gateways.log" 2>&1 || true
  emit_cell_summary "$run_id" "$broker" "$load" "$SHARDS" "$rooms" "$shard_failed"
  if [ "$shard_failed" -ne 0 ] || ! ls "results/$run_id"/shard-*/summary.json >/dev/null 2>&1; then
    if ls "results/$run_id"/shard-*/summary.json >/dev/null 2>&1; then
      echo "cell $run_id failed (shard_failed=$shard_failed)" | tee -a "results/$run_id/cell-failed.txt" >&2
    else
      echo "cell $run_id produced no results (shard_failed=$shard_failed)" | tee -a "results/$run_id/cell-failed.txt" >&2
    fi
    failed_cells+=("$run_id")
    continue
  fi
done 3< <(tail -n +2 "$MATRIX")
