import { OT_COLLECTION } from './constants';

const ot = require('sharedb/lib/ot');

interface ShareOp {
  c?: string;
  d?: string;
  v?: number;
  create?: { type: string; data: unknown };
  del?: boolean;
  op?: unknown;
  src?: string;
  seq?: number;
  x?: unknown;
}

/**
 * ShareDB's pubsub fans an op out to other servers but does not write it into
 * their database. With the default in-memory DB each gateway has its own op
 * log, so a client that fails over would fetch an empty document.
 * Subscribing to the collection channel and applying ops in version order
 * makes every gateway's op log a projection of the same commits. Client
 * catch-up is `doc.fetch` / `subscribe` against that log, not a transport replay.
 */
export function projectRemoteOp(db: MemoryLike, op: ShareOp | null): void {
  if (!op || op.c == null || op.d == null || typeof op.v !== 'number') return;
  if (!db._pending) db._pending = new Map();
  const key = `${op.c}\0${op.d}`;
  const version = db._getVersionSync(op.c, op.d);
  if (op.v < version) return;
  if (op.v > version) {
    const buffered = db._pending.get(key) ?? new Map<number, ShareOp>();
    buffered.set(op.v, op);
    db._pending.set(key, buffered);
    return;
  }
  applyCommitted(db, op, op.c, op.d);
  const buffered = db._pending.get(key);
  if (!buffered) return;
  let next = op.v + 1;
  while (buffered.has(next)) {
    const queued = buffered.get(next)!;
    buffered.delete(next);
    if (queued.c == null || queued.d == null) continue;
    applyCommitted(db, queued, queued.c, queued.d);
    next += 1;
  }
  if (buffered.size === 0) db._pending.delete(key);
}

export function attachOpLogProjection(pubsub: PubSubLike, db: MemoryLike): Promise<void> {
  if (typeof db._writeOpSync !== 'function') return Promise.resolve();
  if (!db._pending) db._pending = new Map();
  return new Promise((resolve, reject) => {
    pubsub.subscribe(OT_COLLECTION, (err: Error | null, stream: OpStream) => {
      if (err) {
        reject(err);
        return;
      }
      stream.on('data', (op: ShareOp) => {
        try {
          projectRemoteOp(db, op);
        } catch (error) {
          console.error('ot project', error);
        }
      });
      resolve();
    });
  });
}

function applyCommitted(db: MemoryLike, op: ShareOp, collection: string, id: string): void {
  const snapshot = db._getSnapshotSync(collection, id, true);
  const failed = ot.apply(snapshot, op);
  if (failed) {
    console.error(`ot project skipped ${collection}.${id} v${op.v}: ${failed.message ?? failed}`);
    return;
  }
  db._writeOpSync(collection, id, op);
  db._writeSnapshotSync(collection, id, snapshot);
}

interface MemoryLike {
  _pending?: Map<string, Map<number, ShareOp>>;
  _getVersionSync(collection: string, id: string): number;
  _getSnapshotSync(collection: string, id: string, metadata: boolean): { v: number; type: string | null; data: unknown };
  _writeOpSync(collection: string, id: string, op: ShareOp): void;
  _writeSnapshotSync(collection: string, id: string, snapshot: unknown): void;
}

interface OpStream {
  on(event: 'data', handler: (op: ShareOp) => void): void;
}

interface PubSubLike {
  subscribe(channel: string, callback: (err: Error | null, stream: OpStream) => void): void;
}
