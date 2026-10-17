import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { evaluateRooms, reliability } from '@domain/reliability';
import { preciseNowMs } from './clock';
import { loadConfig } from './config';
import { DisconnectInjector } from './disconnect-injector';
import { assessLoad } from './load-report';
import { LatencyRecorder } from './latency-recorder';
import { PoissonSchedule } from './poisson-schedule';
import { seededRandom } from './random';
import { ShardScheduler } from './shard-scheduler';
import { ClientCounters, SimulatedClient } from './simulated-client';
import { YjsReplica } from './yjs-replica';

const sleep = (s: number): Promise<void> => new Promise((r) => setTimeout(r, s * 1000));

async function main(): Promise<void> {
  const config = loadConfig();
  const random = seededRandom(config.SEED + config.SHARD_INDEX);
  const gateways = config.GATEWAY_URLS.split(',');
  const counters: ClientCounters = { sent: 0, received: 0, skippedWhileOffline: 0, catchUpEvents: 0, catchUpUnsupported: 0 };
  const recorder = new LatencyRecorder(random, config.RESERVOIR);

  const createdAt = preciseNowMs();
  let shardRooms = 0;
  const clients: SimulatedClient[] = [];
  for (let r = 0; r < config.ROOMS; r++) {
    if (r % config.SHARD_COUNT !== config.SHARD_INDEX) continue;
    shardRooms++;
    for (let c = 0; c < config.CLIENTS_PER_ROOM; c++) {
      clients.push(new SimulatedClient(
        `c${r}-${c}`, `${config.RUN_ID}-r${r}`, gateways,
        new YjsReplica(seededRandom(config.SEED * 7919 + r * 1000 + c)),
        new PoissonSchedule(seededRandom(config.SEED * 104729 + r * 1000 + c), config.RATE_PER_CLIENT, createdAt),
        recorder, counters,
      ));
    }
  }
  if (clients.length === 0) {
    console.log('no rooms assigned to this shard; nothing to do');
    process.exit(0);
  }
  const poolCount = Math.ceil(config.RATE_PER_CLIENT * (config.WARMUP_S + config.DURATION_S) * 1.5) + 32;
  const prebuildStarted = preciseNowMs();
  clients.forEach((c) => c.prepare(poolCount));
  console.log(`prebuilt ${clients.length * poolCount} yjs updates in ${Math.round(preciseNowMs() - prebuildStarted)}ms`);
  await Promise.all(clients.map((c) => c.open()));
  const online = clients.filter((c) => c.isOnline).length;
  console.log(`connected ${online}/${clients.length} clients to ${gateways.length} gateway(s)`);
  if (online === 0) {
    console.error('no clients connected');
    process.exit(1);
  }

  const startMs = preciseNowMs();
  clients.forEach((c) => c.arm(startMs, config.WARMUP_S, config.DURATION_S));
  const injector = new DisconnectInjector(random, {
    ratePerMinute: config.DISCONNECTS_PER_MIN,
    meanOfflineSeconds: config.MEAN_OFFLINE_S,
  });
  clients.forEach((c) => c.startGenerating());
  injector.start(clients);
  const scheduler = new ShardScheduler(clients);
  scheduler.start();
  await sleep(config.WARMUP_S + config.DURATION_S);
  scheduler.stop();
  clients.forEach((c) => c.stopGenerating());
  await injector.stopAndRestore(clients);
  await sleep(config.DRAIN_S);

  const digests = clients.map((c) => c.digest());
  const rooms = evaluateRooms(digests);
  const latency = recorder.summary();
  const generationS = Math.max(0.001, config.WARMUP_S + config.DURATION_S);
  const load = assessLoad({
    offeredPerSec: shardRooms * config.CLIENTS_PER_ROOM * config.RATE_PER_CLIENT,
    achievedSendPerSec: counters.sent / generationS,
    achievedDeliveryPerSec: counters.received / generationS,
    scheduleLagP50Ms: latency.scheduleLagP50Ms ?? 0,
    serviceP50Ms: latency.p50Ms ?? 0,
    disconnectsPerMin: config.DISCONNECTS_PER_MIN,
  });
  if (load.saturated) console.error(`SATURATED ${config.RUN_ID} shard ${config.SHARD_INDEX}: ${load.saturationReason}`);
  const summary = {
    runId: config.RUN_ID,
    broker: config.BROKER_LABEL,
    config,
    counters: { ...counters, disconnectsInjected: injector.injected },
    latency,
    load,
    rooms: rooms.length,
    divergedRooms: rooms.filter((r) => r.diverged).length,
    reliability: reliability(rooms),
  };
  const directory = join(config.OUT_DIR, config.RUN_ID, `shard-${config.SHARD_INDEX}`);
  mkdirSync(directory, { recursive: true });
  recorder.writeCsv(directory);
  writeFileSync(join(directory, 'rooms.csv'), 'room,replicas,diverged\n' +
    rooms.map((r) => `${r.roomId},${r.replicas},${r.diverged ? 1 : 0}`).join('\n') + '\n');
  writeFileSync(join(directory, 'replicas.csv'), 'room,client,digest\n' +
    digests.map((d) => `${d.roomId},${d.clientId},${d.digest}`).join('\n') + '\n');
  writeFileSync(join(directory, 'summary.json'), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify({
    broker: summary.broker, latency: summary.latency, load: summary.load, R: summary.reliability, counters: summary.counters,
  }));
  clients.forEach((c) => c.close());
  process.exit(0);
}

void main().catch((error) => {
  console.error(error);
  process.exit(1);
});
