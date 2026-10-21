import { attachOpLogProjection } from './project-ops';
import { forwardOpMeta } from './forward-meta';

const ShareDB = require('sharedb');

export type OtDbKind = 'memory' | 'postgres';

/**
 * In-memory DB is the default OT component (ShareDB MemoryDB, per gateway).
 * `postgres` selects sharedb-postgres; the schema is that package's structure.sql.
 * Postgres is shared, so ops are not projected into a second log.
 */
export function createOtDatabase(kind: OtDbKind, databaseUrl?: string): any {
  if (kind === 'memory') return new ShareDB.MemoryDB();
  const PostgresDB = require('sharedb-postgres');
  return new PostgresDB(databaseUrl ? { connectionString: databaseUrl } : {});
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
  await attachOpLogProjection(pubsub, db);
  return backend;
}
