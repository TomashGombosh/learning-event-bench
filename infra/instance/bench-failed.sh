#!/usr/bin/env bash
# Upload whatever the bench managed to write, then power the instance off.
# run-bench.sh calls this from its EXIT trap; systemd also starts it via OnFailure=.
set -uo pipefail

if [ -f /etc/leb/bench.env ]; then
  # shellcheck disable=SC1091
  source /etc/leb/bench.env
fi

if [ -x /opt/leb/sync-results.sh ]; then
  /opt/leb/sync-results.sh || true
fi

if [ -n "${RESULTS_BUCKET:-}" ] && [ -n "${RUN_ID:-}" ] && command -v aws >/dev/null 2>&1; then
  region="${AWS_REGION:-us-east-1}"
  dest="s3://${RESULTS_BUCKET}/runs/${RUN_ID}"
  echo "{\"status\":\"failed\",\"finished_at\":\"$(date -Is)\",\"git_sha\":\"${GIT_SHA:-}\"}" > /tmp/DONE.json
  aws s3 cp /tmp/DONE.json "${dest}/DONE.json" --region "$region" || true
  if [ -f /var/log/leb/bench.log ]; then
    aws s3 cp /var/log/leb/bench.log "${dest}/logs/bench.log" --region "$region" || true
  fi
fi

# A shutdown is already queued (the 41 h safety timer). logind then rejects PowerOff
# ("Transaction ... is destructive") and `shutdown -h now` returns without powering off.
shutdown -c || true
shutdown -h now "leb-bench failed; terminating" || true
sleep 15
systemctl poweroff --force || true
sleep 5
poweroff -f || true
exit 0
