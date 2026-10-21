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
