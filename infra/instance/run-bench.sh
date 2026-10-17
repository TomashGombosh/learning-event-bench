#!/usr/bin/env bash
# Runs the experiment matrix on the EC2 instance (started by systemd unit leb-bench.service).
# Environment (from /etc/leb/bench.env): RESULTS_BUCKET, RUN_ID, BENCH_REPS, BENCH_DURATION_S, AWS_REGION, GIT_SHA
set -uo pipefail
mkdir -p /var/log/leb
exec > >(tee -a /var/log/leb/bench.log) 2>&1
source /etc/leb/bench.env
REPO=/home/ubuntu/learning-event-bench
DEST="s3://${RESULTS_BUCKET}/runs/${RUN_ID}"
cd "$REPO"
mkdir -p results

# Compose defaults pin CPUs 0-15 (16 vCPU). c7g.2xlarge has 8; a cpuset past nproc-1
# makes Docker refuse to start the container. Layout for 8 vCPU matches docs/EC2.md.
ncpu="$(nproc)"
if [ "$ncpu" -ge 16 ]; then
  export CPUSET_GATEWAYS="${CPUSET_GATEWAYS:-0-5}"
  export CPUSET_REDIS="${CPUSET_REDIS:-6-7}"
  export CPUSET_KAFKA="${CPUSET_KAFKA:-8-9}"
  export CPUSET_LOADGEN="${CPUSET_LOADGEN:-10-15}"
  export SHARDS="${SHARDS:-4}"
elif [ "$ncpu" -ge 8 ]; then
  export CPUSET_GATEWAYS="${CPUSET_GATEWAYS:-0-2}"
  export CPUSET_REDIS="${CPUSET_REDIS:-3}"
  export CPUSET_KAFKA="${CPUSET_KAFKA:-4}"
  export CPUSET_LOADGEN="${CPUSET_LOADGEN:-5-7}"
  export SHARDS="${SHARDS:-2}"
elif [ "$ncpu" -ge 4 ]; then
  export CPUSET_GATEWAYS="${CPUSET_GATEWAYS:-0}"
  export CPUSET_REDIS="${CPUSET_REDIS:-1}"
  export CPUSET_KAFKA="${CPUSET_KAFKA:-2}"
  export CPUSET_LOADGEN="${CPUSET_LOADGEN:-3}"
  export SHARDS="${SHARDS:-1}"
else
  last="$((ncpu - 1))"
  export CPUSET_GATEWAYS="${CPUSET_GATEWAYS:-0-${last}}"
  export CPUSET_REDIS="${CPUSET_REDIS:-0-${last}}"
  export CPUSET_KAFKA="${CPUSET_KAFKA:-0-${last}}"
  export CPUSET_LOADGEN="${CPUSET_LOADGEN:-0-${last}}"
  export SHARDS="${SHARDS:-1}"
fi

write_meta() {
  cat > results/run-metadata.json <<JSON
{"run_id":"${RUN_ID}","git_sha":"${GIT_SHA}","status":"$1","instance_type":"$(cloud-init query ds.meta_data.instance-type 2>/dev/null || echo unknown)",
 "instance_id":"$(cloud-init query ds.meta_data.instance-id 2>/dev/null || echo unknown)","reps":${BENCH_REPS},"duration_s":${BENCH_DURATION_S},
 "updated_at":"$(date -Is)"}
JSON
}

stop_watchdog() {
  if [ -n "${watchdog_pid:-}" ]; then
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true
    watchdog_pid=""
  fi
}

on_exit() {
  local rc=$?
  stop_watchdog
  if [ "$rc" -eq 0 ]; then
    return 0
  fi
  echo "leb-bench failed (exit $rc)"
  write_meta failed || true
  if [ -x /opt/leb/bench-failed.sh ]; then
    /opt/leb/bench-failed.sh || true
  else
    "$REPO/infra/instance/bench-failed.sh" || true
  fi
}
trap on_exit EXIT

write_meta running
/opt/leb/sync-results.sh
# shellcheck source=experiments/lib/budget.sh
source "$REPO/experiments/lib/budget.sh"
DURATION_S="${BENCH_DURATION_S}"
apply_window_defaults "$DURATION_S"
export DURATION_S WARMUP_S DRAIN_S KILL_AT_S
CELL_TIMEOUT_S="$(cell_timeout_seconds "$WARMUP_S" "$DURATION_S" "$DRAIN_S")"
export CELL_TIMEOUT_S
COMPOSE_UP_TIMEOUT_S="${COMPOSE_UP_TIMEOUT_S:-180}"
watchdog_pid=""
# Root (see leb-bench.service). docker.sock does not use the ubuntu docker group.
# BuildKit records git metadata; the repo is owned by ubuntu, so root needs safe.directory.
# --system does not need HOME (systemd still sets HOME=/root on the unit).
if ! git config --system --get-all safe.directory 2>/dev/null | grep -qxF '*'; then
  git config --system --add safe.directory '*'
fi
if ! git config --system --get-all safe.directory 2>/dev/null | grep -qxF "$REPO"; then
  git config --system --add safe.directory "$REPO"
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose plugin is not on PATH"
  exit 1
fi
# Only gateway1 has `build:`. Building every service tags the same image in parallel
# and BuildKit fails with "image already exists".
if ! docker compose build gateway1; then
  echo "docker compose build failed"
  exit 1
fi
# bench_reps=1 is the smoke knob used by short live runs. The full factorial at
# reps=1 is still ~32 cells (stage A is 2 reps; B and C are 4×3 and 3×2×2), and
# each cell paid a Kafka cold start plus 30s warmup, so "smoke" ran for hours.
if [ "${BENCH_SMOKE:-0}" = "1" ] || [ "${BENCH_REPS}" = "1" ]; then
  echo "smoke matrix: bench_reps=${BENCH_REPS} selects a handful of cells (reps>=2 is the full factorial)"
  (cd experiments && python3 make_matrix.py smoke)
else
  (cd experiments && python3 make_matrix.py "${BENCH_REPS}")
fi
cells="$(tail -n +2 experiments/configs/matrix.csv | grep -c . || true)"
# One cell may spend the compose-up budget plus the loadgen timeout. The watchdog
# is that wall time times the matrix, plus a few minutes to upload. It calls the
# same path as a failed bench: sync results and bench.log to S3, then power off.
watchdog_s=$(( cells * (COMPOSE_UP_TIMEOUT_S + CELL_TIMEOUT_S + 45) + 300 ))
echo "watchdog ${watchdog_s}s for ${cells} cells (per-cell loadgen timeout ${CELL_TIMEOUT_S}s)"
(
  sleep "$watchdog_s"
  echo "global watchdog: wall clock ${watchdog_s}s exceeded; uploading logs and powering off"
  if [ -x /opt/leb/bench-failed.sh ]; then
    /opt/leb/bench-failed.sh
  else
    "$REPO/infra/instance/bench-failed.sh"
  fi
) &
watchdog_pid=$!
./experiments/run_matrix.sh experiments/configs/matrix.csv 2>&1 | tee -a results/run.log
matrix_rc=${PIPESTATUS[0]}
if [ "$matrix_rc" -ne 0 ] || ! ls results/*/shard-*/summary.json >/dev/null 2>&1; then
  echo "benchmark produced no cell results (matrix exit ${matrix_rc})"
  exit 1
fi
STATUS=completed

.venv/bin/python analysis/analyze.py results experiments/configs/matrix.csv >> results/run.log 2>&1 || true
write_meta "$STATUS"
cp experiments/configs/matrix.csv results/matrix.csv
/opt/leb/sync-results.sh
echo "{\"status\":\"${STATUS}\",\"finished_at\":\"$(date -Is)\",\"git_sha\":\"${GIT_SHA}\"}" > /tmp/DONE.json
aws s3 cp /tmp/DONE.json "${DEST}/DONE.json" --region "${AWS_REGION}"
# Replace the 41 h safety shutdown. If logind rejects it, power off anyway.
shutdown -c || true
if ! shutdown -h +5 "benchmark finished (${STATUS}); terminating in 5 minutes"; then
  sleep 5
  systemctl poweroff --force || poweroff -f || true
fi
