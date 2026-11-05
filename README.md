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
python3 experiments/make_matrix.py smoke    # 17 cells. Also: kafka-rerun, or 5 for the full factorial
./experiments/run_matrix.sh                 # resumable; full and kafka-rerun can abort on the validity gate
.venv/bin/python analysis/analyze.py results experiments/configs/matrix.csv
```

### Which run to start

On c7g.4xlarge, in this order. Each step is a `bench-up` workflow dispatch. The input is `profile`.

| Step | `profile` | What runs | Wall clock | What to read before the next step |
|---|---|---|---|---|
| 1 | `smoke` (the default) | 17 cells, 60 s window: one cell per broker × scenario A/B/C, including plain kafka B L5000, kafka-tuned C at both disconnect rates, plain kafka C, `kafka-acks` (acks=all, idempotent off), pubsub C, streams C L5000 | about 45–60 min | CloudWatch `/leb/bench`, stream `<run_id>/bench.log`, line `VALIDITY_SUMMARY`. Also `results/validity.json`. |
| MMC | `mmc-smoke` | 6 cells for the overload model: quiet L200 on each transport, kafka and kafka-tuned at L6000 (T=240), streams L6000 at T=60. Fail-fast is on. | about 40 min | `VALIDITY_SUMMARY`, then `cpu_samples.csv` and `lag_samples.csv` in the cell directory. |
| MMC | `mmc-e1` | Saturation: pubsub, streams, kafka, kafka-tuned × L {200, 1000, 2000, 3000, 3500, 4000, 5000, 6000} × 3 reps, T=240, no faults. 96 cells. | about 9–10 h | Same. A harness `FAIL` on the first cell of a broker aborts. `SATURATED_VALID` does not. |
| MMC | `mmc-e2` | Window length: kafka and kafka-tuned at L5000 and L6000, streams at L6000 only, T in {60, 120, 240, 480} × 3 reps. 60 cells. | about 5.5–6 h | Same. The matrix carries each T; the workflow duration input is 480 so the watchdog covers the longest cell. |
| 2 | `kafka-rerun` | 40 Kafka cells (plain, tuned, `kafka-acks`) at B L3000/L4000/L5000 and C L2500, plus a few C L5000 stress cells. 240 s window. Fail-fast is on. | about 4–5 h | `VALIDITY_SUMMARY` again. A failed canary (the first cell of a broker × scenario) stops the instance. |
| any | `cells` | Only the cells in the `cells` input: run ids of an earlier matrix and/or presets from `experiments/make_matrix.py` (`rerun-20261018-kafka` = the 25 kafka-tuned L5000 cells of run `20261018T110200Z-c440945`, `rerun-20261018-kafka-reliability` = its 20 stage C cells, `rerun-20261018-streams-c` = the 20 Streams stage C cells). Ids are kept. 240 s window, fail-fast on. | about 7 min per cell plus 15 min | `VALIDITY_SUMMARY`, then `bench-results` with `run_id` = the rerun and `base_run_id` = the run it replaces. The finished cells overwrite the base cells and `results/merged-from.json` lists them. |
| 3 | `full` | Factorial, `bench_reps=5`, 240 s window. Stage C includes plain kafka and both L2500 and L5000. Fail-fast is on. | about 23 h if every canary passes | `VALIDITY_SUMMARY`, then `bench-results`. |

Fail-fast (`VALIDITY_FAIL_FAST`, default 1 for `kafka-rerun`, `cells`, and `full`, **0 for `smoke`**) aborts after the first cell of a broker × scenario only on a harness fault: attempted rate under 99%, catch-up errors above `VALIDITY_CATCHUP_ERROR_MAX` (default 10), fewer clients connected than expected, a reservoir that does not cover the window while delivery was complete, or a missing shard. A smoke run therefore finishes with `aborted=0` even if some cells are marked `SATURATED_VALID` or `DIVERGED`. That is deliberate: smoke exists to record every risky cell. `SATURATED_VALID` (generator kept up, and delivery fell more than 0.02 below `1 - offline`) is a measured saturation point, not an abort. A shortfall inside that tolerance is `CLIENTS_OFFLINE` on every broker. Pubsub `DIVERGED` is expected, because that broker cannot replay. The instance writes `DONE.json` with `status=aborted_invalid` and a `reason` only when fail-fast fires, then uploads results and shuts down.

`bench-results` uploads `results/validity.json`, each `results/<run>/shard-*/summary.json`, and each `results/<run>/gateways.log` with the analysis tables.

See [docs/AUTOMATION.md](docs/AUTOMATION.md) for fully automated runs (GitHub Actions + OIDC + Terraform, results in S3, auto-destroy after 41 h) and [docs/EC2.md](docs/EC2.md) for manual runs.

### Configuration (environment variables)

| Component | Variable | Default | Meaning |
|---|---|---|---|
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
(Algorithm L reservoir of raw samples, uniform over the whole record window `[warmup, warmup+duration)`, with uplink/downlink split and scheduled time), `rooms.csv`, `replicas.csv`.
`latency.reservoirEligible` is how many in-window deliveries the reservoir drew from, and `latency.reservoirKept` is how many rows were stored (at most `RESERVOIR`, default 50000). Each CSV row has equal weight and stands for `reservoirEligible / reservoirKept` deliveries. The CSV columns are unchanged. HDR percentiles still count every delivery.
`results/validity.json` is the PASS/FAIL table for the run.
`results/<run>/cpu_samples.csv` is one row per second per container (`ts`, `container`, `cpu_seconds_cumulative`, `cpu_fraction`) for the gateways, Redis, Kafka, and the load generator. Cumulative time comes from cgroup `cpu.stat` `usage_usec`; `docker stats` is the fallback. `cpu_fraction` is core-seconds per wall-second and can exceed 1.
`results/<run>/lag_samples.csv` is consumer lag (`ts`, `broker`, `consumer`, `partition`, `log_end`, `read_position`, `lag`). Kafka lag is log end offset minus the gateway group's read position. Redis Streams lag is `XINFO GROUPS`, or the entries after the gateway cursor when there is no group. Pub/Sub has no backlog, so `lag` is `n/a`.
`summary.json` `censoring` counts events scheduled in `[warmup, warmup+duration)` that were delivered to zero, some, or all other clients in the room by cutoff `C = warmup + duration + drain`. `neverDelivered` is the zero-recipient count. Events still in flight at C are the censored tail.

## Known limitations
- Kafka catch-up of a recent offset is served from an in-memory ring on the live consumer (size `STREAM_MAXLEN`). A short-lived consumer group is opened only when the requested offset is older than that ring.
- WebSocket runs over TCP, so `netem` packet loss turns into delay rather than lost events; divergence is
  produced by disconnects and gateway failures (see the reliability model).
- Latency of re-sent outbox operations is measured from their original scheduled time (time-to-visibility).

## Citing

Author and repository metadata are in [`CITATION.cff`](CITATION.cff). Zenodo deposit metadata is in [`.zenodo.json`](.zenodo.json). The DOI will appear here after the first Zenodo release.

## License
MIT
