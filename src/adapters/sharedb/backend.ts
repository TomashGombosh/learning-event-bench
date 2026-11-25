import { attachVersionGapRecovery } from './gap-recovery';
import { attachOpLogProjection } from './project-ops';
import { forwardOpMeta } from './forward-meta';

const ShareDB = require('sharedb');

export type OtDbKind = 'memory' | 'postgres';

/**
 * `postgres` is the shared op log: one database for every gateway.
 * `memory` is the per-gateway MemoryDB ablation. It does not survive a missed
 * pubsub message, because that process has no other copy of the op.
 */
export function createOtDatabase(kind: OtDbKind, databaseUrl?: string): any {
  if (kind === 'memory') return new ShareDB.MemoryDB();
  if (!databaseUrl) throw new Error('OT_DB=postgres requires DATABASE_URL');
  const PostgresDB = require('sharedb-postgres');
  return new PostgresDB({ connectionString: databaseUrl });
}

/** One ShareDB backend. `pubsub` is a ShareDB PubSub (memory, Redis, Streams, or Kafka). */
export async function createOtBackend(pubsub: any, db: any): Promise<any> {
  forwardOpMeta();
  const backend = new ShareDB({
    db,
    pubsub,
    presence: false,
    doNotForwardSendPresenceErrorsToClient: true,
  });
  // SubmitRequest deletes `op.x` before commit. Timing lives in `x.source`,
  // so put it back: the committed op (catch-up) and the published op (live) both keep it.
  backend.use('commit', (request: { extra?: unknown; op?: { x?: unknown } }, next: (err?: unknown) => void) => {
    if (request.extra != null && request.op) request.op.x = request.extra;
    next();
  });
  if (typeof db._writeOpSync === 'function') {
    // Ablation: each gateway's MemoryDB is filled from pubsub, in version order.
    await attachOpLogProjection(pubsub, db);
  } else {
    // Shared database. A hole in the pubsub stream is read back from that log.
    attachVersionGapRecovery(pubsub, db);
  }
  // Node throws when EventEmitter emits 'error' with no listener. A publish
  // that fails after the op is committed (broker closed, gateway shutting
  // down) must not take the process down. The op is already in the database.
  if (typeof pubsub.on === 'function') {
    pubsub.on('error', (err: unknown) => {
      console.error('ot pubsub', err);
    });
  }
  return backend;
}
