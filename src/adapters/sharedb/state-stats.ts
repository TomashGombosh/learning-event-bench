/** Bytes retained by one gateway's ShareDB op log, split into text and the rest. */
export interface OtStateStats {
  readonly db: 'memory' | 'postgres';
  readonly docs: number;
  readonly textBytes: number;
  /** JSON size of the op log (the state a reconnecting client reads). */
  readonly stateBytes: number;
  /** Op-log bytes that are not the inserted characters: versions, paths, src, timing. */
  readonly metadataBytes: number | null;
}

interface OpComponent {
  si?: string;
}

interface StoredOp {
  op?: OpComponent[];
}

interface MemoryDb {
  ops?: Record<string, Record<string, StoredOp[]>>;
  docs?: Record<string, Record<string, { data?: { text?: unknown } }>>;
}

export function memoryStateStats(db: MemoryDb): OtStateStats {
  let textBytes = 0;
  let stateBytes = 0;
  let insertedBytes = 0;
  let docs = 0;
  for (const [collection, byId] of Object.entries(db.ops ?? {})) {
    for (const [id, opLog] of Object.entries(byId ?? {})) {
      docs += 1;
      const ops = opLog ?? [];
      stateBytes += Buffer.byteLength(JSON.stringify(ops));
      for (const op of ops) {
        if (!op?.op) continue;
        for (const component of op.op) {
          if (typeof component?.si === 'string') insertedBytes += Buffer.byteLength(component.si);
        }
      }
      const text = db.docs?.[collection]?.[id]?.data?.text;
      if (typeof text === 'string') textBytes += Buffer.byteLength(text);
    }
  }
  return {
    db: 'memory',
    docs,
    textBytes,
    stateBytes,
    metadataBytes: Math.max(0, stateBytes - insertedBytes),
  };
}

interface Queryable {
  query(text: string): Promise<{ rows: Array<Record<string, string | number | null>> }>;
}

/** Op-log size in the shared Postgres database. `si` characters count as text, the rest as metadata. */
export async function postgresStateStats(db: { _pool?: Queryable }): Promise<OtStateStats> {
  const pool = db._pool;
  if (!pool) return { db: 'postgres', docs: 0, textBytes: 0, stateBytes: 0, metadataBytes: null };
  const ops = await pool.query(`
    SELECT COALESCE(SUM(octet_length(operation::text)), 0) AS state_bytes,
           COUNT(DISTINCT (collection, doc_id)) AS docs
    FROM ops`);
  const inserted = await pool.query(`
    SELECT COALESCE(SUM(octet_length(elem->>'si')), 0) AS inserted
    FROM ops
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE WHEN jsonb_typeof(operation->'op') = 'array' THEN operation->'op' ELSE '[]'::jsonb END
    ) AS elem
    WHERE jsonb_typeof(elem->'si') = 'string'`);
  const text = await pool.query(`
    SELECT COALESCE(SUM(octet_length(data->>'text')), 0) AS text_bytes
    FROM snapshots`);
  const stateBytes = Number(ops.rows[0]?.state_bytes ?? 0);
  const insertedBytes = Number(inserted.rows[0]?.inserted ?? 0);
  return {
    db: 'postgres',
    docs: Number(ops.rows[0]?.docs ?? 0),
    textBytes: Number(text.rows[0]?.text_bytes ?? 0),
    stateBytes,
    metadataBytes: Math.max(0, stateBytes - insertedBytes),
  };
}
