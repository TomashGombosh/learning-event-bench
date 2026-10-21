import { AddressInfo } from 'node:net';
import { createServer } from 'node:http';
import { createOtBackend, createOtDatabase } from '@adapters/sharedb/backend';
import { createShareDbPubSub } from '@adapters/sharedb/pubsub-factory';
import { attachOtServer } from '../../apps/gateway/ot-server';
import { hashCode } from '../../apps/load-generator/gateway-failover';
import { LatencyRecorder } from '../../apps/load-generator/latency-recorder';
import { PoissonSchedule } from '../../apps/load-generator/poisson-schedule';
import { seededRandom } from '../../apps/load-generator/random';
import { SharedbClient } from '../../apps/load-generator/sharedb-client';
import { ClientCounters } from '../../apps/load-generator/simulated-client';
import { brokersConfigured, ensureOtSchema, truncateOtLog } from './pg-harness';

const live = brokersConfigured() ? describe : describe.skip;

function clientIdFor(slot: number): string {
  for (let i = 0; i < 400; i++) {
    const id = `c${slot}-${i}`;
    if (Math.abs(hashCode(id)) % 2 === slot) return id;
  }
  throw new Error(`no client id for gateway ${slot}`);
}

function counters(): ClientCounters {
  return { sent: 0, received: 0, sentInWindow: 0, skippedWhileOffline: 0, catchUpEvents: 0, catchUpUnsupported: 0 };
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timeout waiting for condition');
    await new Promise((r) => setTimeout(r, 30));
  }
}

interface Gateway {
  url: string;
  /** Drop the websocket, the same thing a client sees when the process is killed. */
  kill(): Promise<void>;
  close(): Promise<void>;
}

async function bootGateway(broker: string): Promise<Gateway> {
  const db = createOtDatabase('postgres', process.env.DATABASE_URL);
  const bundle = await createShareDbPubSub(broker, {
    redisUrl: process.env.REDIS_URL ?? '',
    kafkaBrokers: (process.env.KAFKA_BROKERS ?? '').split(','),
    kafkaPartitions: 1,
    streamMaxLen: 1_000,
  });
  const backend = await createOtBackend(bundle.pubsub, db);
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  const sockets = attachOtServer(server, backend);
  const port = (server.address() as AddressInfo).port;
  let killed = false;
  let closed = false;
  const kill = async (): Promise<void> => {
    if (killed) return;
    killed = true;
    await sockets.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  };
  const close = async (): Promise<void> => {
    if (closed) return;
    closed = true;
    await kill();
    await bundle.close();
    if (typeof db.close === 'function') await new Promise<void>((resolve) => db.close(() => resolve()));
  };
  return { url: `http://127.0.0.1:${port}`, kill, close };
}

live('ShareDB cell on real transports', () => {
  jest.setTimeout(120_000);

  beforeAll(async () => {
    await ensureOtSchema();
  });

  beforeEach(async () => {
    await truncateOtLog();
  });

  it.each(['pubsub', 'streams', 'kafka-tuned'])(
    '%s converges after a gateway kill and a reconnect',
    async (broker) => {
      const first = await bootGateway(broker);
      const second = await bootGateway(broker);
      const urls = [first.url, second.url];
      const recorder = new LatencyRecorder(() => 0, 100);
      const leftCounts = counters();
      const rightCounts = counters();
      const left = new SharedbClient(clientIdFor(0), `room-${broker}`, urls, seededRandom(1), new PoissonSchedule(seededRandom(2), 5, 0), recorder, leftCounts);
      const right = new SharedbClient(clientIdFor(1), `room-${broker}`, urls, seededRandom(3), new PoissonSchedule(seededRandom(4), 5, 0), recorder, rightCounts);
      try {
        await left.open();
        await right.open();
        left.arm(0, 0, 1_000_000);
        right.arm(0, 0, 1_000_000);
        left.startGenerating();
        right.startGenerating();
        left.sendDue(1e12);
        right.sendDue(1e12);
        await waitFor(() => left.digest().digest === right.digest().digest && leftCounts.sent + rightCounts.sent > 0, 15_000);
        right.disconnect();
        left.sendDue(1e15);
        await first.kill();
        await right.reconnect();
        await waitFor(() => {
          return left.isOnline && right.isOnline && left.digest().digest === right.digest().digest;
        }, 25_000);
        expect(left.digest().digest).toBe(right.digest().digest);
        expect(rightCounts.catchUpEvents + rightCounts.received).toBeGreaterThan(0);
      } finally {
        left.close();
        right.close();
        await first.close();
        await second.close();
      }
    },
  );
});
