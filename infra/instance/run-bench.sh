#!/usr/bin/env bash
# Runs the experiment matrix on the EC2 instance (started by systemd unit leb-bench.service).
# Environment (from /etc/leb/bench.env): RESULTS_BUCKET, RUN_ID, BENCH_REPS, BENCH_DURATION_S, AWS_REGION, GIT_SHA
set -uo pipefail
source /etc/leb/bench.env
REPO=/home/ubuntu/learning-event-bench
DEST="s3://${RESULTS_BUCKET}/runs/${RUN_ID}"
cd "$REPO"
mkdir -p results

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
