import { createServer as createHttpServer } from 'node:http';
import { AddressInfo, createServer as createNetServer, Socket as NetSocket } from 'node:net';
import { WebSocket, WebSocketServer } from 'ws';
import { createOtBackend, createOtDatabase } from '@adapters/sharedb/backend';
import { createShareDbPubSub } from '@adapters/sharedb/pubsub-factory';
import { attachOtServer } from '../../apps/gateway/ot-server';
import { DisconnectInjector } from '../../apps/load-generator/disconnect-injector';
import { LatencyRecorder } from '../../apps/load-generator/latency-recorder';
import { PoissonSchedule } from '../../apps/load-generator/poisson-schedule';
import { seededRandom } from '../../apps/load-generator/random';
import { ShardScheduler } from '../../apps/load-generator/shard-scheduler';
import { SharedbClient } from '../../apps/load-generator/sharedb-client';
import { browserSocket, waitUntilOpen } from '../../apps/load-generator/sharedb-socket';
import { ClientCounters } from '../../apps/load-generator/simulated-client';

const ShareDB = require('sharedb');
const ShareDBClient = require('sharedb/lib/client');
const otJson0 = require('ot-json0');

function makeCounters(): ClientCounters {
  return { sent: 0, received: 0, sentInWindow: 0, skippedWhileOffline: 0, catchUpEvents: 0, catchUpUnsupported: 0 };
}

async function waitFor(predicate: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timeout waiting for condition');
    await new Promise((r) => setTimeout(r, 20));
  }
}

describe('SharedbClient resilience and crash regressions', () => {
  it('(a) closing a CONNECTING WebSocket via disconnect/close or timeout does not throw unhandled error', async () => {
    const heldSockets: NetSocket[] = [];
    // Accept TCP connections but never answer the HTTP upgrade, leaving WebSocket in CONNECTING.
    const stalledServer = createNetServer((socket) => {
      heldSockets.push(socket);
    });
    await new Promise<void>((resolve) => stalledServer.listen(0, '127.0.0.1', () => resolve()));
    const stalledPort = (stalledServer.address() as AddressInfo).port;
    const stalledUrl = `http://127.0.0.1:${stalledPort}`;

    // Also test waitUntilOpen timeout + closing CONNECTING ws directly with error guard.
    const rawWs = new WebSocket(`ws://127.0.0.1:${stalledPort}/sharedb`);
    let caughtError = false;
    rawWs.on('error', () => {
      caughtError = true;
    });
    await expect(waitUntilOpen(rawWs, 60)).rejects.toThrow(/connect timeout/);
    rawWs.close();
    await new Promise((r) => setTimeout(r, 40));
    expect(caughtError).toBe(true);

    // Real gateway on second URL so SharedbClient fails over after the stalled gateway times out.
    const db = createOtDatabase('memory');
    const bundle = await createShareDbPubSub('memory', {
      redisUrl: 'redis://localhost:6379', kafkaBrokers: ['localhost:9092'], kafkaPartitions: 1, streamMaxLen: 100,
    });
    const backend = await createOtBackend(bundle.pubsub, db);
    const httpServer = createHttpServer();
    await new Promise<void>((resolve) => httpServer.listen(0, '127.0.0.1', () => resolve()));
    const otSockets = attachOtServer(httpServer, backend);
    const healthyPort = (httpServer.address() as AddressInfo).port;
    const healthyUrl = `http://127.0.0.1:${healthyPort}`;

    const recorder = new LatencyRecorder(() => 0, 100);
    // Client 1: disconnect() and close() while CONNECTING to stalled server.
    const c1 = new SharedbClient(
      'c-connecting-close',
      'room-a',
      [stalledUrl],
      seededRandom(1),
      new PoissonSchedule(seededRandom(2), 5, 0),
      recorder,
      makeCounters(),
    );
    const openPromise = c1.open();
    await waitFor(() => heldSockets.length >= 1);
    c1.disconnect();
    c1.close();
    await expect(openPromise).resolves.toBeUndefined();

    // Client 2: waits through CONNECT_TIMEOUT_MS on stalled gateway and fails over to healthy gateway.
    const c2 = new SharedbClient(
      'c-timeout-failover',
      'room-a',
      [stalledUrl, healthyUrl],
      seededRandom(3),
      new PoissonSchedule(seededRandom(4), 5, 0),
      recorder,
      makeCounters(),
    );
    try {
      await c2.open();
      expect(c2.isOnline).toBe(true);
    } finally {
      c2.close();
      for (const s of heldSockets) s.destroy();
      stalledServer.close();
      await otSockets.close();
      httpServer.close();
    }
  });

  it('(b) late subscribe response after dropSocket/disconnect does not crash Doc._handleSubscribe', async () => {
    const wss = new WebSocketServer({ noServer: true });
    const httpServer = createHttpServer();
    httpServer.on('upgrade', (req, socket, head) => {
      wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
    });
    await new Promise<void>((resolve) => httpServer.listen(0, '127.0.0.1', () => resolve()));
    const port = (httpServer.address() as AddressInfo).port;
    const url = `http://127.0.0.1:${port}`;

    let serverWs: WebSocket | null = null;
    wss.on('connection', (ws) => {
      serverWs = ws;
      ws.on('message', (raw) => {
        const msg = JSON.parse(raw.toString()) as { a?: string };
        if (msg.a === 'hs') {
          ws.send(JSON.stringify({ a: 'hs', id: 'srv-1', protocol: 1, protocolMinor: 1, type: 'http://sharejs.org/types/JSONv0' }));
        }
        // Intentionally delay `{a: 's'}` so client disconnects first, then send `{a: 's'}`.
      });
    });

    const client = new SharedbClient(
      'c-late-sub',
      'room-b',
      [url],
      seededRandom(10),
      new PoissonSchedule(seededRandom(11), 5, 0),
      new LatencyRecorder(() => 0, 100),
      makeCounters(),
    );

    const openPromise = client.open();
    await waitFor(() => serverWs !== null);
    // Wait briefly so ShareDB sends `{a: 's'}` and has inflightSubscribe active.
    await new Promise((r) => setTimeout(r, 40));
    // Trigger dropSocket() via disconnect(), then immediately send a late `{a: 's'}` frame.
    client.disconnect();
    serverWs!.send(JSON.stringify({ a: 's', c: 'rooms', d: 'room-b', data: { v: 0 } }));
    await new Promise((r) => setTimeout(r, 40));
    client.close();
    await expect(openPromise).resolves.toBeUndefined();

    // Also test browserSocket directly: once readyState is CLOSED, messages are ignored.
    const clientWs = new WebSocket(`ws://127.0.0.1:${port}/sharedb`);
    await waitUntilOpen(clientWs, 2000);
    const shareSock = browserSocket(clientWs);
    const conn = new ShareDBClient.Connection(shareSock);
    const doc = conn.get('rooms', 'direct');
    let subCalled = 0;
    doc.subscribe(() => {
      subCalled++;
    });
    shareSock.onmessage = null;
    shareSock.readyState = WebSocket.CLOSED;
    shareSock.onclose?.('disconnected');
    serverWs!.send(JSON.stringify({ a: 's', c: 'rooms', d: 'direct', data: { v: 0 } }));
    await new Promise((r) => setTimeout(r, 30));
    expect(subCalled).toBe(0);
    conn.close();

    wss.close();
    httpServer.close();
  });

  it('(c) 20 concurrent clients in one room and dual-creator race produce zero ERR_DOC_ALREADY_CREATED errors', async () => {
    const db = new ShareDB.MemoryDB();
    const origCommit = db.commit.bind(db);
    // Delay commits slightly so concurrent creates collide if not serialized.
    db.commit = (collection: unknown, id: unknown, op: unknown, snapshot: unknown, options: unknown, cb: unknown) => {
      setTimeout(() => origCommit(collection, id, op, snapshot, options, cb), 15);
    };
    const pubsub = new ShareDB.MemoryPubSub();
    const backend = await createOtBackend(pubsub, db);
    const server = createHttpServer();
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    const sockets = attachOtServer(server, backend);
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
    const recorder = new LatencyRecorder(() => 0, 100);

    // Part 1: 20 clients with recipientIndex 0..19 in one room.
    const clients = Array.from({ length: 20 }, (_, i) => new SharedbClient(
      `c0-${i}`,
      'room-20',
      [url],
      seededRandom(100 + i),
      new PoissonSchedule(seededRandom(200 + i), 5, 0),
      recorder,
      makeCounters(),
      null,
      i,
      19,
    ));

    // Part 2: 2 clients both with recipientIndex=0 racing to create 'room-race'.
    const racerA = new SharedbClient('racer-a', 'room-race', [url], seededRandom(301), new PoissonSchedule(seededRandom(302), 5, 0), recorder, makeCounters(), null, 0, 1);
    const racerB = new SharedbClient('racer-b', 'room-race', [url], seededRandom(303), new PoissonSchedule(seededRandom(304), 5, 0), recorder, makeCounters(), null, 0, 1);

    try {
      await Promise.all(clients.map((c) => c.open()));
      await Promise.all([racerA.open(), racerB.open()]);
      expect(clients.every((c) => c.isOnline)).toBe(true);
      expect(racerA.isOnline && racerB.isOnline).toBe(true);

      const errorText = errorSpy.mock.calls.map((args) => args.map(String).join(' ')).join('\n');
      expect(errorText).not.toMatch(/Document was created remotely|ERR_DOC_ALREADY_CREATED/);
    } finally {
      errorSpy.mockRestore();
      clients.forEach((c) => c.close());
      racerA.close();
      racerB.close();
      await sockets.close();
      server.close();
    }
  });

  it('(d) fast json0.transformX matches ot-json0.transform and survives saturated DB + gateway kill + failover', async () => {
    // Instantiate one client to ensure installOpHook installed transformX on defaultType.
    const dummy = new SharedbClient(
      'dummy',
      'r',
      ['http://127.0.0.1:1'],
      seededRandom(1),
      new PoissonSchedule(seededRandom(2), 1, 0),
      new LatencyRecorder(() => 0, 10),
      makeCounters(),
    );
    dummy.close();

    const json0 = ShareDBClient.types.defaultType;
    expect(typeof json0.transformX).toBe('function');
    const rnd = seededRandom(42);
    for (let i = 0; i < 200; i++) {
      const a = [{ p: ['text', Math.floor(rnd() * 50)], si: i % 3 === 0 ? 'xy' : 'a' }];
      const b = [{ p: ['text', Math.floor(rnd() * 50)], si: 'b' }];
      const [lx, rx] = json0.transformX(a, b);
      expect(lx).toEqual(otJson0.type.transform(a, b, 'left'));
      expect(rx).toEqual(otJson0.type.transform(b, a, 'right'));
    }
    // Non-si fallback (delete / multi-component).
    const delOp = [{ p: ['text', 2], sd: 'ab' }];
    const insOp = [{ p: ['text', 0], si: 'z' }];
    const [dl, dr] = json0.transformX(delOp, insOp);
    expect(dl).toEqual(otJson0.type.transform(delOp, insOp, 'left'));
    expect(dr).toEqual(otJson0.type.transform(insOp, delOp, 'right'));

    // Small-scale saturated multi-gateway cell with DisconnectInjector and gateway kill.
    const pubsub = new ShareDB.MemoryPubSub();
    const db = new ShareDB.MemoryDB();
    const origCommit = db.commit.bind(db);
    db.commit = (collection: unknown, id: unknown, op: unknown, snapshot: unknown, options: unknown, cb: unknown) => {
      setTimeout(() => origCommit(collection, id, op, snapshot, options, cb), 30);
    };
    const backend1 = await createOtBackend(pubsub, db);
    const backend2 = await createOtBackend(pubsub, db);
    const s1 = createHttpServer();
    const s2 = createHttpServer();
    await Promise.all([
      new Promise<void>((r) => s1.listen(0, '127.0.0.1', () => r())),
      new Promise<void>((r) => s2.listen(0, '127.0.0.1', () => r())),
    ]);
    const sock1 = attachOtServer(s1, backend1);
    const sock2 = attachOtServer(s2, backend2);
    const urls = [
      `http://127.0.0.1:${(s1.address() as AddressInfo).port}`,
      `http://127.0.0.1:${(s2.address() as AddressInfo).port}`,
    ];

    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
    const recorder = new LatencyRecorder(seededRandom(99), 500);
    const clients: SharedbClient[] = [];
    for (let r = 0; r < 2; r++) {
      for (let c = 0; c < 10; c++) {
        clients.push(new SharedbClient(
          `c${r}-${c}`,
          `room-${r}`,
          urls,
          seededRandom(1000 + r * 20 + c),
          new PoissonSchedule(seededRandom(2000 + r * 20 + c), 5, Date.now()),
          recorder,
          makeCounters(),
          null,
          c,
          9,
        ));
      }
    }
    const scheduler = new ShardScheduler(clients);
    const injector = new DisconnectInjector(seededRandom(77), { ratePerMinute: 12, meanOfflineSeconds: 0.25 });
    try {
      await Promise.all(clients.map((c) => c.open()));
      const start = Date.now();
      clients.forEach((c) => c.arm(start, 0.2, 2.5));
      clients.forEach((c) => c.startGenerating());
      scheduler.start();
      injector.start(clients);
      await new Promise((r) => setTimeout(r, 1200));
      // Kill gateway2 mid-run while clients are generating and disconnecting.
      await sock2.close();
      s2.close();
      await new Promise((r) => setTimeout(r, 1500));
      clients.forEach((c) => c.stopGenerating());
      await injector.stopAndRestore(clients);
      scheduler.stop();
      await waitFor(() => clients.every((c) => c.isOnline), 10_000);
      await waitFor(() => {
        for (let r = 0; r < 2; r++) {
          const digests = new Set(clients.slice(r * 10, (r + 1) * 10).map((c) => c.digest().digest));
          if (digests.size !== 1) return false;
        }
        return true;
      }, 10_000);

      expect(errorSpy).not.toHaveBeenCalled();
    } finally {
      errorSpy.mockRestore();
      scheduler.stop();
      clients.forEach((c) => c.close());
      await sock1.close();
      s1.close();
    }
  });
});
