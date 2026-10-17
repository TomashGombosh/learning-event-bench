# learning-event-bench

Experimental stand for the article *"Queueing and Reliability Model of Real-Time Synchronisation in Scalable
Interactive Learning Environments: Experimental Validation for Redis Pub/Sub, Redis Streams and Apache Kafka"*
(T. Hombosh, P. Mulesa, Uzhhorod National University). It continues
[Hombosh & Mulesa, 2026, vol. 49](https://doi.org/10.24144/2616-7700.2026.49(2).173-180) by measuring
end-to-end latency **T** and synchronisation reliability **R = 1 − N_diverged / N_total** of Yjs (CRDT)
collaborative sessions under three message brokers. A room is diverged when its online replicas do not
share one digest, **or when any client in the room is lost**. A client is lost (`clients_lost`) when it
is offline at the end of the cell (the connect failed, or reconnect never reached a live gateway).
That room does not count as converged just because the clients that stayed up agree. A client is not
converged (`clients_not_converged`) when it is lost, or when it is online but its digest is not the
strict majority of the room (every client, if there is no majority). `clients_lost` is included in
`clients_not_converged`. Both are in `summary.json` and in `results/_analysis/runs.csv`.

## Architecture (Clean Architecture, R. C. Martin)

```
src/
  domain/            Entities & value objects: CollabEvent, EventCategory, LatencySample, ReplicaDigest, R
  application/
    ports/           EventBusPort, ReplayableEventLogPort (segregated: Pub/Sub cannot replay), RealtimeTransportPort, ClockPort
    use-cases/       PublishEvent, RelayRoomEvents, CatchUpAfterReconnect
  adapters/          in-memory, redis-pubsub, redis-streams, kafka, socketio, broker-factory
apps/
  gateway/           NestJS + Socket.IO composition root (thin controller, DI wires adapters into use cases)
  load-generator/    Simulated students: Yjs replicas, open-loop Poisson schedule, disconnect injector, HDR metrics
test/
  architecture/      Dependency-rule test (domain -> nothing, application -> domain only)
  contract/          One Liskov contract suite run against every EventBusPort adapter
  unit/              Domain, use cases, Poisson schedule, Yjs convergence/divergence
experiments/         make_matrix.py (randomised run plan), run_matrix.sh (Docker), smoke-local.sh (no Docker)
analysis/            model.py (M/G/1, M/M/1, reliability formulas, minimal window L*), analyze.py (statistics)
docs/                EC2.md (step-by-step AWS run), adr/ (architecture decisions)
infra/terraform/     Optional Terraform for the EC2 instance (see its README)
```

Dependency rule: source dependencies point inwards only (`apps -> adapters -> application -> domain`),
enforced by `test/architecture/dependency-rule.test.ts`.

### How a collaborative edit flows
1. A simulated client edits its Yjs document and emits `op` (with its *scheduled* send time) to a gateway.
2. `PublishEventUseCase` stamps the gateway time and appends the event to the broker (`EventBusPort`).
3. Every gateway relays room events to its local sockets (`RelayRoomEventsUseCase`).
4. Receivers apply the Yjs update and record `T = received − scheduled` (same process clock, no clock skew).
5. On reconnect a client sends its last seen event id; `CatchUpAfterReconnectUseCase` replays missed events
   (Redis Streams: `XRANGE`; Kafka: short-lived consumer seeking the room partition; Pub/Sub: unsupported).
6. Clients keep an outbox of unacknowledged operations and re-send them after reconnecting
   (Yjs updates are idempotent, so duplicates are harmless). Each client starts on
   `hash(clientId) % N` of `GATEWAY_URLS`. A dropped session leaves that URL for 30 s
   (`docker compose kill` does not restart the container) and keeps dialing the other
   gateways, with a cap of 32 handshakes in flight, until one accepts.
7. At the end, every replica reports a SHA-256 digest of its text. A room has diverged if those digests
   disagree, or if any of its clients is offline (`clients_lost`). See the definition of R above.

## Quick start (no Docker)

Requirements: Node.js 20, Redis 7 on `localhost:6379`, Kafka (KRaft) on `localhost:9092`, Python 3.11+.

```bash
npm ci
npm test                                   # unit + architecture + in-memory contract
REDIS_URL=redis://localhost:6379 KAFKA_BROKERS=localhost:9092 npm test   # + Redis and Kafka contracts
./experiments/smoke-local.sh               # 2 gateways x 3 brokers, 25 clients, 20 s, disconnects 6/min
python3 -m venv .venv && .venv/bin/pip install -r analysis/requirements.txt pytest
.venv/bin/python -m pytest analysis/test_model.py
```

## Docker / full experiment

```bash
docker compose build
python3 experiments/make_matrix.py          # 128 randomised runs (stages A, B, C)
./experiments/run_matrix.sh                 # ~12 h on c7i.4xlarge, resumable
.venv/bin/python analysis/analyze.py results experiments/configs/matrix.csv
```

See [docs/AUTOMATION.md](docs/AUTOMATION.md) for fully automated runs (GitHub Actions + OIDC + Terraform, results in S3, auto-destroy after 41 h) and [docs/EC2.md](docs/EC2.md) for manual runs.

### Configuration (environment variables)

| Component | Variable | Default | Meaning |
|---|---|---|---|
| gateway | `BROKER` | `memory` | `memory`, `pubsub`, `streams`, `kafka` (acks=all, idempotent), `kafka-tuned` (acks=1) |
| gateway | `STREAM_MAXLEN` | `10000` | Redis Streams replay window *L* |
| gateway | `KAFKA_PARTITIONS` | `12` | partitions of topic `collab-events` |
| load generator | `ROOMS`, `CLIENTS_PER_ROOM`, `RATE_PER_CLIENT` | 5, 5, 5 | Λ = rooms · n · λ_u |
| load generator | `DISCONNECTS_PER_MIN`, `MEAN_OFFLINE_S` | 0, 2 | ν and δ of the reliability model |
| load generator | `WARMUP_S`, `DURATION_S`, `DRAIN_S` | 5, 30, 5 | measurement window |
| load generator | `SHARD_INDEX`, `SHARD_COUNT` | 0, 1 | split rooms over several generator processes |
| load generator | `NETEM` | – | e.g. `delay 10ms 5ms distribution normal` (needs `NET_ADMIN`) |

### Outputs (per run and shard: `results/<run>/shard-<i>/`)
`summary.json` (config, counters, HDR percentiles, R, `clientsLost`, `clientsNotConverged`), `latency_percentiles.csv`, `latency_samples.csv`
(uniform reservoir of raw samples with uplink/downlink split), `rooms.csv`, `replicas.csv`.

## Known limitations
- Kafka catch-up uses a short-lived consumer group (≈3–6 s group join); this is part of what is measured.
- WebSocket runs over TCP, so `netem` packet loss turns into delay rather than lost events; divergence is
  produced by disconnects and gateway failures (see the reliability model).
- Latency of re-sent outbox operations is measured from their original scheduled time (time-to-visibility).

## License
MIT
