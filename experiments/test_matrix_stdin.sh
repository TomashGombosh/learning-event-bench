#!/usr/bin/env bash
# Proves run_matrix.sh still walks every matrix row when `docker` drains stdin.
# The broker reset between cells is `docker compose exec`; Compose attaches stdin
# even with -T. If that stdin is the matrix loop, the run exits 0 after one reset.
# Stdin here is /dev/zero, so a missing </dev/null hangs and the timeout fails the test.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/experiments/lib" "$tmp/bin" "$tmp/results"
cp "$root/experiments/run_matrix.sh" "$tmp/experiments/run_matrix.sh"
cp "$root/experiments/validity.py" "$tmp/experiments/validity.py"
cp "$root/experiments/sample_cell.py" "$tmp/experiments/sample_cell.py"
cp "$root/experiments/lib/budget.sh" "$tmp/experiments/lib/budget.sh"
chmod +x "$tmp/experiments/run_matrix.sh"

cat > "$tmp/matrix.csv" <<'CSV'
stage,broker,load,rooms,clients,rate,nu,kill,rep,run_id
A,kafka,1,1,1,1,0,0,1,cell-000
A,pubsub,1,1,1,1,0,0,1,cell-001
A,streams,1,1,1,1,0,0,1,cell-002
A,kafka,1,1,1,1,0,0,1,cell-003
B,pubsub,1,1,1,1,0,0,1,cell-004
C,streams,1,1,1,1,0,0,1,cell-005
CSV

cat > "$tmp/bin/docker" <<'STUB'
#!/usr/bin/env bash
# Drain stdin the way `docker compose exec -T` does. /dev/null ends immediately;
# a leaked matrix fd or /dev/zero does not.
cat >/dev/null
printf '%s\n' "$*" >> "${DOCKER_LOG:?}"
if [[ "$*" == *"--list"* ]]; then
  n=0
  if [ -f "${LIST_COUNT:?}" ]; then
    n=$(cat "$LIST_COUNT")
  fi
  echo $((n + 1)) > "$LIST_COUNT"
  # First two lists still show the topic so the reset must retry, not rebuild Kafka.
  if [ "$n" -lt 2 ]; then
    echo collab-events
  fi
fi
if [[ "$*" == *"loadgen"* ]]; then
  run_id=$(printf '%s\n' "$*" | sed -n 's/.*RUN_ID=\([^ ]*\).*/\1/p')
  shard=$(printf '%s\n' "$*" | sed -n 's/.*SHARD_INDEX=\([^ ]*\).*/\1/p')
  dir="results/${run_id}/shard-${shard}"
  mkdir -p "$dir"
  printf '%s\n' '{"load":{"offeredPerSec":1,"achievedSendPerSec":1,"saturated":false},"latency":{"p50Ms":1,"p99Ms":1},"divergedRooms":0,"clientsLost":0,"clientsNotConverged":0}' \
    > "$dir/summary.json"
fi
exit 0
STUB
chmod +x "$tmp/bin/docker"

export PATH="$tmp/bin:$PATH"
export DOCKER_LOG="$tmp/docker.log"
export LIST_COUNT="$tmp/list-count"
export NETEM=
export SHARDS=1
export DURATION_S=1
export CELL_TIMEOUT_S=5
export COMPOSE_UP_TIMEOUT_S=5
unset WARMUP_S DRAIN_S KILL_AT_S
: > "$DOCKER_LOG"

set +e
timeout 45 "$tmp/experiments/run_matrix.sh" "$tmp/matrix.csv" </dev/zero > "$tmp/matrix.out" 2> "$tmp/matrix.err"
rc=$?
set -e
echo "matrix_rc=$rc"
cat "$tmp/matrix.out"
echo '--- stderr ---'
cat "$tmp/matrix.err"
if [ "$rc" -eq 124 ]; then
  echo "run_matrix hung reading stdin" >&2
  exit 1
fi
if [ "$rc" -ne 0 ]; then
  echo "run_matrix exit $rc" >&2
  exit 1
fi
grep -qx 'CELLS_SUMMARY executed=6/expected=6 failed=0' "$tmp/matrix.out"
test "$(grep -c '^=== cell-' "$tmp/matrix.out")" -eq 6
# One down at the first cell (brokers were not up) and one from the EXIT trap.
test "$(grep -c 'down -v' "$DOCKER_LOG")" -eq 2
grep -q -- '--delete --topic collab-events' "$DOCKER_LOG"
if grep -q 'recreating the stack' "$tmp/matrix.out" "$tmp/matrix.err"; then
  echo "topic reset fell back to a full recreate" >&2
  exit 1
fi
python3 - <<PY
import json
d = json.load(open("$tmp/results/cells-summary.json"))
assert d == {"executed": 6, "expected": 6, "failed": 0}, d
PY
echo "matrix stdin test passed"
