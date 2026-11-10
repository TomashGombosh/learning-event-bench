import { attachVersionGapRecovery } from '@adapters/sharedb/gap-recovery';
import { createOtBackend, createOtDatabase } from '@adapters/sharedb/backend';
import { postgresStateStats } from '@adapters/sharedb/state-stats';
import { ensureOtSchema, postgresConfigured, truncateOtLog } from './pg-harness';

const ShareDB = require('sharedb');

const live = postgresConfigured() ? describe : describe.skip;

live('Postgres op log', () => {
  const room = `gap-${process.pid}`;

  beforeAll(async () => {
    await ensureOtSchema();
    await truncateOtLog();
  });

  it('fills a skipped pubsub version from the shared database', async () => {
    const db = createOtDatabase('postgres', process.env.DATABASE_URL);
    const pubA = new ShareDB.MemoryPubSub();
    const backend = await createOtBackend(pubA, db);
    const doc = backend.connect().get('rooms', room);
    await new Promise<void>((resolve, reject) => {
      doc.subscribe((err: Error | null) => {
        if (err) return reject(err);
        doc.create({ text: '' }, (createErr: Error | null) => {
          if (createErr) return reject(createErr);
          doc.submitOp([{ p: ['text', 0], si: 'a' }], (firstErr: Error | null) => {
            if (firstErr) return reject(firstErr);
            doc.submitOp([{ p: ['text', 1], si: 'b' }], (secondErr: Error | null) => {
              if (secondErr) reject(secondErr);
              else resolve();
            });
          });
        });
      });
    });

    const stored = await new Promise<Array<{ v: number; c?: string; d?: string }>>((resolve, reject) => {
      db.getOps('rooms', room, 0, null, {}, (err: Error | null, ops: Array<{ v: number }>) => {
        if (err) reject(err);
        else resolve(ops);
      });
    });
    expect(stored.map((op) => op.v)).toEqual([0, 1, 2]);

    const pubB = new ShareDB.MemoryPubSub();
    attachVersionGapRecovery(pubB, db);
    const seen: number[] = [];
    await new Promise<void>((resolve, reject) => {
      pubB.subscribe(`rooms.${room}`, (err: Error | null, stream: { on(event: string, fn: (op: { v: number }) => void): void }) => {
        if (err) return reject(err);
        stream.on('data', (op) => seen.push(op.v));
        resolve();
      });
    });
    pubB._emit(`rooms.${room}`, { ...stored[0], c: 'rooms', d: room });
    pubB._emit(`rooms.${room}`, { ...stored[2], c: 'rooms', d: room });
    const deadline = Date.now() + 8_000;
    while (seen.length < 3) {
      if (Date.now() > deadline) throw new Error(`seen ${seen.join(',')}`);
      await new Promise((r) => setTimeout(r, 20));
    }
    expect(seen).toEqual([0, 1, 2]);

    const stats = await postgresStateStats(db);
    expect(stats.db).toBe('postgres');
    expect(stats.docs).toBe(1);
    expect(stats.textBytes).toBe(2);
    expect(stats.metadataBytes).toBeGreaterThan(0);
    await new Promise<void>((resolve) => db.close(() => resolve()));
  });
});
