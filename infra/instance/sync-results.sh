#!/usr/bin/env bash
# Periodic upload of results to S3 (leb-sync.timer, every 10 min, and at the end of the run). Never deletes.
set -uo pipefail
source /etc/leb/bench.env
cd /home/ubuntu/learning-event-bench
aws s3 sync results "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/results" --region "${AWS_REGION}" --only-show-errors \
  --exclude ".log-*"
aws s3 cp /var/log/leb-setup.log "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/logs/setup.log" --region "${AWS_REGION}" --only-show-errors || true
journalctl -u leb-bench --no-pager > /tmp/leb-bench.journal 2>&1 || true
aws s3 cp /tmp/leb-bench.journal "s3://${RESULTS_BUCKET}/runs/${RUN_ID}/logs/bench.journal" --region "${AWS_REGION}" --only-show-errors || true
