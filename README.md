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
   A first join sends null. The gateway does not page the log (that opened a Kafka consumer group per
   client). It returns the current tail id as `cursor`. The client stores that id, so an outage before
   any live event still has a cursor and the events after it are replayed. An empty log's cursor is the
   start of the log (`0` in memory, `0-0` on Streams, `${partition}--1` on Kafka).
6. Clients keep an outbox of unacknowledged operations and re-send them after reconnecting
   (Yjs updates are idempotent, so duplicates are harmless). Each client starts on
   `hash(clientId) % N` of `GATEWAY_URLS`. A dropped session leaves that URL for 30 s
   (`docker compose kill` does not restart the container) and keeps dialing the other
   gateways, with a cap of 32 handshakes in flight, until one accepts.
7. At the end, every replica reports a SHA-256 digest of its text. A room has diverged if those digests
   disagree, or if any of its clients is offline (`clients_lost`). See the definition of R above.
   `rooms.csv` and `replicas.csv` use the same columns for Yjs and ShareDB.

### Operational Transformation (ShareDB)

`SCHEME=sharedb` runs ShareDB (MIT) with the json0 text type (`{text: ""}`) instead of Yjs.
The ShareDB server is the gateway. Simulated clients use `sharedb/lib/client` on a websocket at
`/sharedb` on the same gateway port. **The default OT component is one shared Postgres database
(`sharedb-postgres`) for every gateway.** Docker Compose starts that database and applies
`infra/postgres/structure.sql`. Catch-up is `subscribe` / `fetch` against that op log. It is not a
Redis `XRANGE` or a Kafka replay. When a gateway sees a pubsub op whose version skips ahead, it
reads the missing ops from Postgres and forwards them locally before the newer one.
`OT_DB=memory` is the per-gateway `MemoryDB` ablation: that process has no other copy of an op, so
a missed pubsub message stays missing. The ablation still projects ops into its local log.

Inter-gateway propagation is a ShareDB PubSub: `sharedb-redis-pubsub` for Redis Pub/Sub, and
in-tree adapters for Redis Streams and Kafka (KafkaJS, the same producer settings as the Yjs
transport, including `kafka-tuned`: acks=1, idempotent off). Those adapters start at the tail.
`BROKER=memory` is in-process only and does not cross gateway processes. `SCHEME=sharedb` does
not also start the Yjs broker.

Timing rides in ShareDB `x.source` (`scheduledAtMs`, `sentAtMs`, `clientId`). Service latency is
`received − sentAtMs` on the sender's clock. Ops applied before the subscribe callback are
catch-up and are not latency samples. A reconnect uses `bindToSocket`, so an unacknowledged op
keeps its `src`/`seq` and is not submitted again.

`summary.json` `resources` still records load-generator process CPU. The cell line
`cpu_s_per_1000` prefers `results/<run>/cpu_samples.csv` (the PR #21 sampler: one row per second
per container, cgroup `usage_usec`, positive deltas summed, `cpu_source=containers`). A cell with
no sample file falls back to the load-generator process (`cpu_source=loadgen`). State metadata
bytes are Yjs document state minus the UTF-8 text, or the Postgres op-log JSON minus inserted
characters (`GET /bench/state`) plus client text. `npm run complexity` prints LOC
(no tests, no comments) and ESLint cyclomatic complexity per adapter directory. The Redis Pub/Sub
OT adapter is the npm package `sharedb-redis-pubsub`, so it has no in-tree LOC.

### CSA matrix (scheme × transport)

`experiments/csa_profiles.py` is the only place the three provisional profiles are defined:
whiteboard n=20, 5 events/s/user, L=2500; quiz n=30, 0.2 events/s/user; class chat n=50,
0.5 events/s/user. The cross is 6 alternatives (Yjs, ShareDB) × (Pub/Sub, Streams, Kafka-tuned)
× 3 profiles × 2 fault levels (ν=0; ν=1/min and a gateway kill) × 5 reps = 180 cells, seed
`20261021`. Smoke is 12 cells (whiteboard, one rep, both fault levels). Both CSA modes set
`VALIDITY_FAIL_FAST=1`. The generator exits before writing a plan that is internally
inconsistent, and it exits if `run_matrix.sh` no longer emits the `SATURATED` flag.

```bash
python3 experiments/make_csa_matrix.py smoke    # 12 cells, or `full` for 180
./experiments/run_csa.sh smoke                  # writes the plan, then run_matrix.sh
npm run complexity
```

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
python3 experiments/make_matrix.py smoke    # 17 cells. Also: kafka-rerun, or 5 for the full factorial
./experiments/run_matrix.sh                 # resumable; full and kafka-rerun can abort on the validity gate
.venv/bin/python analysis/analyze.py results experiments/configs/matrix.csv
```

### Which run to start

On c7g.4xlarge, in this order. Each step is a `bench-up` workflow dispatch. The input is `profile`.

| Step | `profile` | What runs | Wall clock | What to read before the next step |
|---|---|---|---|---|
| 1 | `smoke` (the default) | 17 cells, 60 s window: one cell per broker × scenario A/B/C, including plain kafka B L5000, kafka-tuned C at both disconnect rates, plain kafka C, `kafka-acks` (acks=all, idempotent off), pubsub C, streams C L5000 | about 45–60 min | CloudWatch `/leb/bench`, stream `<run_id>/bench.log`, line `VALIDITY_SUMMARY`. Also `results/validity.json`. |
| 2 | `kafka-rerun` | 40 Kafka cells (plain, tuned, `kafka-acks`) at B L3000/L4000/L5000 and C L2500, plus a few C L5000 stress cells. 240 s window. Fail-fast is on. | about 4–5 h | `VALIDITY_SUMMARY` again. A failed canary (the first cell of a broker × scenario) stops the instance. |
| any | `cells` | Only the cells in the `cells` input: run ids of an earlier matrix and/or presets from `experiments/make_matrix.py` (`rerun-20261018-kafka` = the 25 kafka-tuned L5000 cells of run `20261018T110200Z-c440945`, `rerun-20261018-kafka-reliability` = its 20 stage C cells, `rerun-20261018-streams-c` = the 20 Streams stage C cells). Ids are kept. 240 s window, fail-fast on. | about 7 min per cell plus 15 min | `VALIDITY_SUMMARY`, then `bench-results` with `run_id` = the rerun and `base_run_id` = the run it replaces. The finished cells overwrite the base cells and `results/merged-from.json` lists them. |
| 3 | `full` | Factorial, `bench_reps=5`, 240 s window. Stage C includes plain kafka and both L2500 and L5000. Fail-fast is on. | about 23 h if every canary passes | `VALIDITY_SUMMARY`, then `bench-results`. |

Fail-fast (`VALIDITY_FAIL_FAST`, default 1 for `kafka-rerun`, `cells`, and `full`, **0 for `smoke`**) aborts after the first cell of a broker × scenario only on a harness fault: attempted rate under 99%, catch-up errors above `VALIDITY_CATCHUP_ERROR_MAX` (default 10), fewer clients connected than expected, a reservoir that does not cover the window while delivery was complete, or a missing shard. A smoke run therefore finishes with `aborted=0` even if some cells are marked `SATURATED_VALID` or `DIVERGED`. That is deliberate: smoke exists to record every risky cell. `SATURATED_VALID` (generator kept up, and delivery fell more than 0.02 below `1 - offline`) is a measured saturation point, not an abort. A shortfall inside that tolerance is `CLIENTS_OFFLINE` on every broker. Pubsub `DIVERGED` is expected, because that broker cannot replay. The instance writes `DONE.json` with `status=aborted_invalid` and a `reason` only when fail-fast fires, then uploads results and shuts down.

`bench-results` uploads `results/validity.json`, each `results/<run>/shard-*/summary.json`, and each `results/<run>/gateways.log` with the analysis tables.

See [docs/AUTOMATION.md](docs/AUTOMATION.md) for fully automated runs (GitHub Actions + OIDC + Terraform, results in S3, auto-destroy after 41 h) and [docs/EC2.md](docs/EC2.md) for manual runs.

### Configuration (environment variables)

| Component | Variable | Default | Meaning |
|---|---|---|---|
| gateway | `SCHEME` | `yjs` | `yjs` or `sharedb`. ShareDB does not start the Yjs broker |
| gateway | `OT_DB` | `postgres` | Shared op log. `memory` is the per-gateway ablation |
| gateway | `BROKER` | `memory` | `memory`, `pubsub`, `streams`, `kafka` (acks=all, idempotent), `kafka-tuned` (acks=1), `kafka-acks` (acks=all, not idempotent) |
| gateway | `STREAM_MAXLEN` | `10000` | Redis Streams replay window *L* |
| gateway | `KAFKA_PARTITIONS` | `12` | partitions of topic `collab-events` |
| load generator | `ROOMS`, `CLIENTS_PER_ROOM`, `RATE_PER_CLIENT` | 5, 5, 5 | Λ = rooms · n · λ_u |
| load generator | `DISCONNECTS_PER_MIN`, `MEAN_OFFLINE_S` | 0, 2 | ν and δ of the reliability model |
| load generator | `WARMUP_S`, `DURATION_S`, `DRAIN_S` | 5, 30, 5 | measurement window |
| load generator | `SHARD_INDEX`, `SHARD_COUNT` | 0, 1 | split rooms over several generator processes |
| load generator | `NETEM` | – | e.g. `delay 10ms 5ms distribution normal` (needs `NET_ADMIN`) |

### Outputs (per run and shard: `results/<run>/shard-<i>/`)
`summary.json` (config, counters, HDR percentiles, delivery ratio, Kafka `producer` `{idempotent, acks, maxInFlight}`, R, `clientsLost`, `clientsNotConverged`), `latency_percentiles.csv`, `latency_samples.csv`
(uniform reservoir of raw samples with uplink/downlink split and scheduled time), `rooms.csv`, `replicas.csv`.
`results/validity.json` is the PASS/FAIL table for the run.

## Known limitations
- Kafka catch-up of a recent offset is served from an in-memory ring on the live consumer (size `STREAM_MAXLEN`). A short-lived consumer group is opened only when the requested offset is older than that ring.
- WebSocket runs over TCP, so `netem` packet loss turns into delay rather than lost events; divergence is
  produced by disconnects and gateway failures (see the reliability model).
- Latency of re-sent outbox operations is measured from their original scheduled time (time-to-visibility).
- ShareDB pubsub adapters start at the tail. With `OT_DB=postgres` a missed version is read back from the shared op log. With `OT_DB=memory` (the ablation) a missed pubsub message is gone. A killed gateway is not restarted.
- CPU seconds are the load-generator process, not the gateway processes. Quiz and class-chat aggregate loads are provisional (`experiments/csa_profiles.py`).

## Citing

Author and repository metadata are in [`CITATION.cff`](CITATION.cff). Zenodo deposit metadata is in [`.zenodo.json`](.zenodo.json). The DOI will appear here after the first Zenodo release.

## License
MIT
