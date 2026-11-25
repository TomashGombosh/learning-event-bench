#!/usr/bin/env bash
# Smoke run without Docker: expects Redis on :6379 and Kafka on :9092 already running.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${OUT_DIR:-results/smoke}
SCHEME=${SCHEME:-yjs}
for BROKER in ${BROKERS:-pubsub streams kafka-tuned}; do
  pids=()
  for PORT in 3001 3002; do
    BROKER=$BROKER SCHEME=$SCHEME PORT=$PORT KAFKA_PARTITIONS=6 setsid npx tsx apps/gateway/main.ts > "/tmp/gw-$BROKER-$PORT.log" 2>&1 &
    pids+=($!)
  done
  for PORT in 3001 3002; do
    for _ in $(seq 120); do grep -q "listening" "/tmp/gw-$BROKER-$PORT.log" && break; sleep 0.5; done; grep -q "listening" "/tmp/gw-$BROKER-$PORT.log" || { cat "/tmp/gw-$BROKER-$PORT.log"; exit 1; }
  done
  GATEWAY_URLS=http://localhost:3001,http://localhost:3002 BROKER_LABEL=$BROKER SCHEME=$SCHEME RUN_ID="smoke-$BROKER-$(date +%s)" \
    ROOMS=${ROOMS:-5} CLIENTS_PER_ROOM=${CLIENTS_PER_ROOM:-5} RATE_PER_CLIENT=${RATE_PER_CLIENT:-5} \
    WARMUP_S=3 DURATION_S=${DURATION_S:-20} DRAIN_S=${DRAIN_S:-10} DISCONNECTS_PER_MIN=${DISCONNECTS_PER_MIN:-6} MEAN_OFFLINE_S=1 \
    OUT_DIR=$OUT npx tsx apps/load-generator/main.ts
  for pid in "${pids[@]}"; do kill -- "-$pid" 2>/dev/null || true; done; sleep 2
done
