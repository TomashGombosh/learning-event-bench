import { AddressInfo } from 'node:net';
import { createServer } from 'node:http';
import { memoryStateStats } from '@adapters/sharedb/state-stats';
import { createOtBackend, createOtDatabase } from '@adapters/sharedb/backend';
import { projectRemoteOp } from '@adapters/sharedb/project-ops';
import { createShareDbPubSub } from '@adapters/sharedb/pubsub-factory';
import { attachOtServer } from '../../apps/gateway/ot-server';
import { LatencyRecorder } from '../../apps/load-generator/latency-recorder';
import { PoissonSchedule } from '../../apps/load-generator/poisson-schedule';
import { seededRandom } from '../../apps/load-generator/random';
import { cpuPerThousand } from '../../apps/load-generator/resources';
import { SharedbClient } from '../../apps/load-generator/sharedb-client';
import { ClientCounters } from '../../apps/load-generator/simulated-client';

const ShareDB = require('sharedb');
const JSON0 = 'http://sharejs.org/types/JSONv0';

function counters(): ClientCounters {
  return { sent: 0, received: 0, sentInWindow: 0, skippedWhileOffline: 0, catchUpEvents: 0, catchUpUnsupported: 0 };
}

async function waitFor(predicate: () => boolean, timeoutMs = 8_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timeout waiting for condition');
    await new Promise((r) => setTimeout(r, 20));
  }
}

describe('OT op-log projection', () => {
  it('applies a buffered op once the missing version arrives', () => {
    const db = new ShareDB.MemoryDB();
    projectRemoteOp(db, {
      c: 'rooms', d: 'r', v: 1, src: 'a', seq: 2, op: [{ p: ['text', 0], si: 'b' }],
    });
    expect(db._getVersionSync('rooms', 'r')).toBe(0);
    projectRemoteOp(db, {
      c: 'rooms', d: 'r', v: 0, src: 'a', seq: 1,
      create: { type: JSON0, data: { text: '' } },
    });
    expect(db._getVersionSync('rooms', 'r')).toBe(2);
    expect(db._getSnapshotSync('rooms', 'r', false).data.text).toBe('b');
    const stats = memoryStateStats(db);
    expect(stats.textBytes).toBe(1);
    expect(stats.metadataBytes).toBeGreaterThan(0);
    expect(stats.stateBytes).toBeGreaterThan(stats.metadataBytes ?? 0);
  });

  it('a second gateway catches up from its projected op log', async () => {
    const Share = ShareDB;
    const pubsub = new Share.MemoryPubSub();
    const dbA = new Share.MemoryDB();
    const dbB = new Share.MemoryDB();
    const backendA = await createOtBackend(pubsub, dbA);
    const backendB = await createOtBackend(pubsub, dbB);
    const docA = backendA.connect().get('rooms', 'room-1');
    await new Promise<void>((resolve, reject) => {
      docA.subscribe((err: Error | null) => {
        if (err) return reject(err);
        docA.create({ text: '' }, (createErr: Error | null) => {
          if (createErr) return reject(createErr);
          docA.submitOp([{ p: ['text', 0], si: 'hi' }], (opErr: Error | null) => (opErr ? reject(opErr) : resolve()));
        });
      });
    });
    await waitFor(() => dbB._getVersionSync('rooms', 'room-1') >= 2);
    const docB = backendB.connect().get('rooms', 'room-1');
    await new Promise<void>((resolve, reject) => {
      docB.subscribe((err: Error | null) => (err ? reject(err) : resolve()));
    });
    expect(docB.data.text).toBe('hi');
    docA.destroy?.();
  });
});

describe('ShareDB websocket clients', () => {
  it('converges, and a reconnect catches up from the op log', async () => {
    const db = createOtDatabase('memory');
    const bundle = await createShareDbPubSub('memory', {
      redisUrl: 'redis://localhost:6379', kafkaBrokers: ['localhost:9092'], kafkaPartitions: 1, streamMaxLen: 100,
    });
    const backend = await createOtBackend(bundle.pubsub, db);
    const server = createServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    const sockets = attachOtServer(server, backend);
    const port = (server.address() as AddressInfo).port;
    const url = `http://127.0.0.1:${port}`;
    const recorder = new LatencyRecorder(() => 0, 100);
    const leftCounts = counters();
    const rightCounts = counters();
    const left = new SharedbClient('left', 'room', [url], seededRandom(1), new PoissonSchedule(seededRandom(2), 5, 0), recorder, leftCounts);
    const right = new SharedbClient('right', 'room', [url], seededRandom(3), new PoissonSchedule(seededRandom(4), 5, 0), recorder, rightCounts);
    try {
      await left.open();
      await right.open();
      left.arm(0, 0, 1_000_000);
      left.startGenerating();
      left.sendDue(1e12);
      await waitFor(() => rightCounts.received >= 1);
      expect(right.digest().digest).toBe(left.digest().digest);
      right.disconnect();
      left.sendDue(1e15);
      await right.reconnect();
      await waitFor(() => right.digest().digest === left.digest().digest && rightCounts.catchUpEvents > 0);
      expect(right.isOnline).toBe(true);
      expect(leftCounts.sent).toBeGreaterThan(rightCounts.received);
    } finally {
      left.close();
      right.close();
      await sockets.close();
      await bundle.close();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});

describe('cell resource metrics', () => {
  it('reports CPU seconds per 1000 events', () => {
    expect(cpuPerThousand({ user: 2_000_000, system: 0 }, 4_000)).toEqual({
      cpuSeconds: 2,
      cpuSecondsPer1000Events: 0.5,
    });
    expect(cpuPerThousand({ user: 0, system: 0 }, 0).cpuSecondsPer1000Events).toBe(0);
  });
});
