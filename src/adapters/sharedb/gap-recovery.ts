interface ShareOp {
  c?: string;
  d?: string;
  v?: number;
  op?: unknown;
  create?: unknown;
  del?: boolean;
  src?: string;
  seq?: number;
  x?: unknown;
}

interface OpsDb {
  getOps(
    collection: string,
    id: string,
    from: number,
    to: number | null,
    options: unknown,
    callback: (err: Error | null, ops?: ShareOp[]) => void,
  ): void;
}

interface EmitPubSub {
  _emit(channel: string, data: unknown): void;
}

const PATCHED = Symbol.for('leb.sharedb.gapRecovery');

/**
 * PubSub is a hint, not the op log. When a gateway observes op.v ahead of the
 * last version it forwarded, it reads the hole from the shared database and
 * emits those ops locally before the newer one. Clients then stay continuous
 * without a transport replay. The first op on a channel is not a gap: history
 * before this process subscribed is already in the snapshot a client fetches.
 */
export function attachVersionGapRecovery(pubsub: EmitPubSub, db: OpsDb): void {
  const target = pubsub as EmitPubSub & { [PATCHED]?: boolean };
  if (target[PATCHED] || typeof target._emit !== 'function' || typeof db.getOps !== 'function') return;
  const original = target._emit.bind(target);
  const expected = new Map<string, number>();
  const queues = new Map<string, ShareOp[]>();
  const draining = new Set<string>();

  target._emit = (channel: string, data: unknown) => {
    const op = data as ShareOp | null;
    if (!op || op.c == null || op.d == null || typeof op.v !== 'number') {
      original(channel, data);
      return;
    }
    // ShareDB publishes the collection channel and the doc channel. Agents
    // listen on the doc channel. Only that one is ordered through the gap fill.
    if (channel !== `${op.c}.${op.d}`) {
      original(channel, data);
      return;
    }
    const key = `${op.c}\0${op.d}`;
    const queue = queues.get(key) ?? [];
    queue.push(op);
    queues.set(key, queue);
    drain(key);
  };
  target[PATCHED] = true;

  function drain(key: string): void {
    if (draining.has(key)) return;
    const queue = queues.get(key);
    if (!queue || queue.length === 0) return;
    const op = queue[0];
    const next = expected.get(key);
    if (next == null || op.v! <= next) {
      queue.shift();
      if (next == null || op.v === next) expected.set(key, op.v! + 1);
      original(`${op.c}.${op.d}`, op);
      drain(key);
      return;
    }
    draining.add(key);
    const from = next;
    db.getOps(op.c!, op.d!, from, op.v!, {}, (err, ops) => {
      draining.delete(key);
      if (!err && ops) {
        for (const missing of ops) {
          if (!missing || typeof missing.v !== 'number') continue;
          if (missing.v < from || missing.v >= op.v!) continue;
          missing.c = missing.c ?? op.c;
          missing.d = missing.d ?? op.d;
          expected.set(key, missing.v + 1);
          original(missing.c!, missing);
          original(`${missing.c}.${missing.d}`, missing);
        }
      }
      const cursor = expected.get(key);
      if (cursor != null && op.v! > cursor) {
        // The hole is not in the db yet. Release the newer op; the client fetches.
        queue.shift();
        expected.set(key, op.v! + 1);
        original(`${op.c}.${op.d}`, op);
      }
      drain(key);
    });
  }
}
