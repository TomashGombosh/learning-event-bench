#!/usr/bin/env bash
# Three short ShareDB cells, one per CSA transport. The last cell kills a gateway.
# This is not the 180-cell plan: one room, two clients, a few seconds.
# VALIDITY_FAIL_FAST stays off so every cell prints a VALIDITY line.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=experiments/configs/csa-local-smoke.csv
mkdir -p experiments/configs
cat > "$OUT" <<'CSV'
stage,broker,load,rooms,clients,rate,disconnects_per_min,gateway_kill,rep,run_id,scheme
whiteboard,pubsub,4,1,2,2,0,0,0,000-whiteboard-sharedb-pubsub-L4-nu0-k0-r0,sharedb
whiteboard,streams,4,1,2,2,0,0,0,001-whiteboard-sharedb-streams-L4-nu0-k0-r0,sharedb
whiteboard,kafka-tuned,4,1,2,2,1,1,0,002-whiteboard-sharedb-kafka-tuned-L4-nu1-k1-r0,sharedb
CSV
export VALIDITY_FAIL_FAST="${VALIDITY_FAIL_FAST:-0}"
export DURATION_S="${DURATION_S:-8}"
export WARMUP_S="${WARMUP_S:-1}"
export DRAIN_S="${DRAIN_S:-2}"
export SHARDS=1
export NETEM="${NETEM-}"
# The committed cpusets assume a 16-core bench host. Pin everything to cpu 0 here.
export CPUSET_GATEWAY1=0 CPUSET_GATEWAY2=0 CPUSET_GATEWAY3=0
export CPUSET_REDIS=0 CPUSET_KAFKA=0 CPUSET_POSTGRES=0 CPUSET_GATEWAYS=0 CPUSET_LOADGEN=0
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256M -Xmx256M}"
docker compose build gateway1
exec ./experiments/run_matrix.sh "$OUT"
