import { kafkaProducerSettings } from '@adapters/broker-factory';
import { closeSync, fsyncSync, mkdirSync, openSync, writeFileSync, writeSync } from 'node:fs';
import { join } from 'node:path';
import { evaluateRooms, reliability, ReplicaDigest } from '@domain/reliability';
import { preciseNowMs } from './clock';
import { loadConfig } from './config';
import { DisconnectInjector } from './disconnect-injector';
import { assessLoad } from './load-report';
import { LatencyRecorder } from './latency-recorder';
import { PoissonSchedule } from './poisson-schedule';
import { seededRandom } from './random';
import { cpuPerThousand, serverMetadataBytes, sumReplicaStats } from './resources';
import { ShardScheduler } from './shard-scheduler';
import { SharedbClient } from './sharedb-client';
import { ClientCounters, SimulatedClient } from './simulated-client';
import { YjsReplica } from './yjs-replica';

interface BenchClient {
  readonly clientId: string;
  readonly isOnline: boolean;
  readonly counters: ClientCounters;
  prepare(count: number): void;
  arm(startMs: number, warmupS: number, durationS: number): void;
  open(): Promise<void>;
  startGenerating(): void;
  stopGenerating(): void;
  sendDue(nowMs: number): void;
  disconnect(): void;
  reconnect(): Promise<void>;
  digest(): ReplicaDigest;
  stateStats(): { textBytes: number; stateBytes: number; metadataBytes: number };
  close(): void;
}

const sleep = (s: number): Promise<void> => new Promise((r) => setTimeout(r, s * 1000));

let stopRequested = false;

/** Poll so SIGTERM can end the window and still fsync a summary before SIGKILL. */
async function sleepUntil(seconds: number): Promise<boolean> {
  const end = preciseNowMs() + seconds * 1000;
  while (!stopRequested && preciseNowMs() < end) {
    const remainingS = (end - preciseNowMs()) / 1000;
    await sleep(Math.min(0.5, Math.max(0, remainingS)));
  }
  return stopRequested;
}

function writeSynced(path: string, body: string): void {
  const fd = openSync(path, 'w');
  try {
    writeSync(fd, body);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

async function drainStdout(line: string): Promise<void> {
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, 2_000);
    process.stdout.write(line.endsWith('\n') ? line : `${line}\n`, () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function main(): Promise<void> {
  const config = loadConfig();
  const random = seededRandom(config.SEED + config.SHARD_INDEX);
  const gateways = config.GATEWAY_URLS.split(',');
  const counters: ClientCounters = {
    sent: 0, received: 0, sentInWindow: 0, skippedWhileOffline: 0, catchUpEvents: 0, catchUpUnsupported: 0,
  };
  const recorder = new LatencyRecorder(random, config.RESERVOIR);
  let injector: DisconnectInjector | null = null;
  process.on('SIGTERM', () => {
    stopRequested = true;
    injector?.stop();
    console.error('SIGTERM: writing summary and exiting');
  });

  const createdAt = preciseNowMs();
  let shardRooms = 0;
  const clients: BenchClient[] = [];
  for (let r = 0; r < config.ROOMS; r++) {
    if (r % config.SHARD_COUNT !== config.SHARD_INDEX) continue;
    shardRooms++;
    for (let c = 0; c < config.CLIENTS_PER_ROOM; c++) {
      const clientId = `c${r}-${c}`;
      const roomId = `${config.RUN_ID}-r${r}`;
      const schedule = new PoissonSchedule(seededRandom(config.SEED * 104729 + r * 1000 + c), config.RATE_PER_CLIENT, createdAt);
      if (config.SCHEME === 'sharedb') {
        clients.push(new SharedbClient(
          clientId, roomId, gateways,
          seededRandom(config.SEED * 7919 + r * 1000 + c),
          schedule, recorder, counters,
        ));
      } else {
        clients.push(new SimulatedClient(
          clientId, roomId, gateways,
          new YjsReplica(seededRandom(config.SEED * 7919 + r * 1000 + c)),
          schedule, recorder, counters,
        ));
      }
    }
  }
  if (clients.length === 0) {
    console.log('no rooms assigned to this shard; nothing to do');
    process.exit(0);
  }
  // 1.15x covers the open-loop schedule; editRandomly is the fallback if the pool runs dry.
  const poolCount = Math.ceil(config.RATE_PER_CLIENT * (config.WARMUP_S + config.DURATION_S) * 1.15) + 32;
  const prebuildStarted = preciseNowMs();
  clients.forEach((c) => c.prepare(poolCount));
  console.log(`prebuilt ${clients.length * poolCount} ${config.SCHEME} updates in ${Math.round(preciseNowMs() - prebuildStarted)}ms`);
  await Promise.all(clients.map((c) => c.open()));
  const online = clients.filter((c) => c.isOnline).length;
  console.log(`connected ${online}/${clients.length} clients to ${gateways.length} gateway(s)`);

  let cpuAt = process.cpuUsage();
  const writeSummary = async (incomplete: boolean): Promise<void> => {
    const digests = clients.map((c) => c.digest());
    const lostIds = new Set(clients.filter((client) => !client.isOnline).map((client) => client.clientId));
    const rooms = evaluateRooms(digests, lostIds);
    const clientsLost = lostIds.size;
    const clientsNotConverged = rooms.reduce((sum, room) => sum + room.notConvergedClients, 0);
    const latency = recorder.summary();
    const generationS = Math.max(0.001, config.WARMUP_S + config.DURATION_S);
    const attempted = counters.sent + counters.skippedWhileOffline;
    const expectedDeliveries = counters.sentInWindow * Math.max(0, config.CLIENTS_PER_ROOM - 1);
    const observedDeliveries = recorder.count;
    const deliveryRatio = expectedDeliveries === 0 ? 1 : observedDeliveries / expectedDeliveries;
    const load = assessLoad({
      offeredPerSec: shardRooms * config.CLIENTS_PER_ROOM * config.RATE_PER_CLIENT,
      achievedSendPerSec: counters.sent / generationS,
      attemptedPerSec: attempted / generationS,
      achievedDeliveryPerSec: counters.received / generationS,
      scheduleLagP50Ms: latency.scheduleLagP50Ms ?? 0,
      serviceP50Ms: latency.p50Ms ?? 0,
    });
    if (load.saturated) console.error(`SATURATED ${config.RUN_ID} shard ${config.SHARD_INDEX}: ${load.saturationReason}`);
    const replicas = sumReplicaStats(clients.map((client) => client.stateStats()));
    const serverMetadata = config.SCHEME === 'sharedb' ? await serverMetadataBytes(gateways) : 0;
    const cpu = cpuPerThousand(process.cpuUsage(cpuAt), counters.sent);
    const summary = {
      runId: config.RUN_ID,
      scheme: config.SCHEME,
      broker: config.BROKER_LABEL,
      incomplete,
      config,
      counters: { ...counters, disconnectsInjected: injector?.injected ?? 0 },
      latency,
      load,
      clientsExpected: clients.length,
      clientsConnected: online,
      delivery: {
        sentInWindow: counters.sentInWindow,
        expected: expectedDeliveries,
        observed: observedDeliveries,
        ratio: deliveryRatio,
        censored: deliveryRatio < 0.99,
      },
      producer: kafkaProducerSettings(config.BROKER_LABEL),
      resources: {
        ...cpu,
        events: counters.sent,
        textBytes: replicas.textBytes,
        stateBytes: replicas.stateBytes,
        metadataBytes: replicas.metadataBytes,
        serverMetadataBytes: serverMetadata,
        stateMetadataBytes: replicas.metadataBytes + serverMetadata,
      },
      rooms: rooms.length,
      divergedRooms: rooms.filter((r) => r.diverged).length,
      clientsLost,
      clientsNotConverged,
      reliability: reliability(rooms),
    };
    if (clientsLost > 0) {
      const sample = [...lostIds].slice(0, 20).join(',');
      console.error(`clients_lost=${clientsLost} clients_not_converged=${clientsNotConverged} sample=${sample}`);
    }
    const directory = join(config.OUT_DIR, config.RUN_ID, `shard-${config.SHARD_INDEX}`);
    mkdirSync(directory, { recursive: true });
    recorder.writeCsv(directory);
    writeFileSync(join(directory, 'rooms.csv'), 'room,replicas,diverged,lost,not_converged\n' +
      rooms.map((r) => `${r.roomId},${r.replicas},${r.diverged ? 1 : 0},${r.lostClients},${r.notConvergedClients}`).join('\n') + '\n');
    writeFileSync(join(directory, 'replicas.csv'), 'room,client,digest\n' +
      digests.map((d) => `${d.roomId},${d.clientId},${d.digest}`).join('\n') + '\n');
    writeSynced(join(directory, 'summary.json'), JSON.stringify(summary, null, 2));
    const line = JSON.stringify({
      scheme: summary.scheme, broker: summary.broker, incomplete,
      latency: summary.latency, load: summary.load, resources: summary.resources, R: summary.reliability,
      rooms_diverged: summary.divergedRooms, clients_lost: summary.clientsLost,
      clients_not_converged: summary.clientsNotConverged, counters: summary.counters,
    });
    await drainStdout(line);
    clients.forEach((c) => c.close());
    process.exit(incomplete ? 1 : 0);
  };

  if (online === 0 || stopRequested) {
    console.error(online === 0 ? 'no clients connected' : 'stopped before the measurement window');
    await writeSummary(true);
    return;
  }

  const startMs = preciseNowMs();
  cpuAt = process.cpuUsage();
  clients.forEach((c) => c.arm(startMs, config.WARMUP_S, config.DURATION_S));
  injector = new DisconnectInjector(random, {
    ratePerMinute: config.DISCONNECTS_PER_MIN,
    meanOfflineSeconds: config.MEAN_OFFLINE_S,
  });
  clients.forEach((c) => c.startGenerating());
  injector.start(clients);
  const scheduler = new ShardScheduler(clients);
  scheduler.start();
  const generationCut = await sleepUntil(config.WARMUP_S + config.DURATION_S);
  scheduler.stop();
  clients.forEach((c) => c.stopGenerating());
  let incomplete = generationCut;
  if (!incomplete) {
    await Promise.race([
      injector.stopAndRestore(clients),
      sleepUntil(config.DRAIN_S + 60),
    ]);
    if (stopRequested) {
      injector.stop();
      incomplete = true;
    } else {
      incomplete = await sleepUntil(config.DRAIN_S);
    }
  } else {
    injector.stop();
  }

  await writeSummary(incomplete || stopRequested);
}

void main().catch((error) => {
  console.error(error);
  process.exit(1);
});
