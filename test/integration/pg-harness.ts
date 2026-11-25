import { readFileSync } from 'node:fs';

interface PgClient {
  connect(): Promise<void>;
  query(text: string): Promise<{ rows: Array<Record<string, unknown>> }>;
  end(): Promise<void>;
}

const Pg = require('pg') as { Client: new (opts: { connectionString?: string }) => PgClient };

export function brokersConfigured(): boolean {
  return Boolean(process.env.REDIS_URL && process.env.KAFKA_BROKERS && process.env.DATABASE_URL);
}

export function postgresConfigured(): boolean {
  return Boolean(process.env.DATABASE_URL);
}

async function withClient<T>(fn: (client: PgClient) => Promise<T>): Promise<T> {
  const client = new Pg.Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

/** Applies sharedb-postgres structure.sql when the ops table is absent. */
export async function ensureOtSchema(): Promise<void> {
  await withClient(async (client) => {
    const found = await client.query("SELECT to_regclass('public.ops') AS name");
    if (found.rows[0]?.name) return;
    await client.query(readFileSync('infra/postgres/structure.sql', 'utf8'));
  });
}

export async function truncateOtLog(): Promise<void> {
  await withClient((client) => client.query('TRUNCATE ops, snapshots'));
}
