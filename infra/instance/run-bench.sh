#!/usr/bin/env bash
# Runs the experiment matrix on the EC2 instance (started by systemd unit leb-bench.service).
# Environment (from /etc/leb/bench.env): RESULTS_BUCKET, RUN_ID, BENCH_REPS, BENCH_DURATION_S, AWS_REGION, GIT_SHA
set -uo pipefail
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

write_meta running
/opt/leb/sync-results.sh
docker compose build 2>&1 | tail -n 5
(cd experiments && python3 make_matrix.py "${BENCH_REPS}")
DURATION_S="${BENCH_DURATION_S}" ./experiments/run_matrix.sh experiments/configs/matrix.csv 2>&1 | tee -a results/run.log
STATUS=$([ "${PIPESTATUS[0]}" = "0" ] && echo completed || echo failed)

.venv/bin/python analysis/analyze.py results experiments/configs/matrix.csv >> results/run.log 2>&1 || true
write_meta "$STATUS"
cp experiments/configs/matrix.csv results/matrix.csv
/opt/leb/sync-results.sh
echo "{\"status\":\"${STATUS}\",\"finished_at\":\"$(date -Is)\",\"git_sha\":\"${GIT_SHA}\"}" > /tmp/DONE.json
aws s3 cp /tmp/DONE.json "${DEST}/DONE.json" --region "${AWS_REGION}"
# Finished early: power off; instance_initiated_shutdown_behavior=terminate removes the instance.
shutdown -h +5 "benchmark finished (${STATUS}); terminating in 5 minutes"
