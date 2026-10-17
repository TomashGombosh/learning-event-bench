# Shared by run_matrix.sh and run-bench.sh.
# A 60s smoke cell used to pay the full-run warmup (30s) and drain (15s) on top of
# a Kafka cold start, so each cell took about five minutes.

apply_window_defaults() {
  local duration=${1:-240}
  if [ -z "${WARMUP_S+x}" ]; then
    if [ "$duration" -le 90 ]; then WARMUP_S=5; else WARMUP_S=30; fi
  fi
  if [ -z "${DRAIN_S+x}" ]; then
    if [ "$duration" -le 90 ]; then DRAIN_S=5; else DRAIN_S=15; fi
  fi
  if [ -z "${KILL_AT_S+x}" ]; then
    KILL_AT_S=$(( WARMUP_S + duration / 2 ))
    if [ "$KILL_AT_S" -lt 1 ]; then KILL_AT_S=1; fi
  fi
  export WARMUP_S DRAIN_S KILL_AT_S
}

# Hard cap for one loadgen process: (warmup + duration + drain) * 3 + margin.
# The margin covers connect and a gateway failover. Healthy cells finish well inside it.
cell_timeout_seconds() {
  local warmup=$1 duration=$2 drain=$3
  local margin=${CELL_TIMEOUT_MARGIN_S:-60}
  echo $(( (warmup + duration + drain) * 3 + margin ))
}
