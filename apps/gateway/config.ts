import { z } from 'zod';

const schema = z.object({
  SCHEME: z.enum(['yjs', 'sharedb']).default('yjs'),
  BROKER: z.enum(['memory', 'pubsub', 'streams', 'kafka', 'kafka-tuned', 'kafka-acks']).default('memory'),
  /**
   * OT persistence. `postgres` (sharedb-postgres) is the shared op log, one
   * database for every gateway. `memory` is the per-gateway MemoryDB ablation.
   */
  OT_DB: z.enum(['memory', 'postgres']).default('postgres'),
  DATABASE_URL: z.string().optional(),
  PORT: z.coerce.number().int().positive().default(3000),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  KAFKA_PARTITIONS: z.coerce.number().int().positive().default(12),
  STREAM_MAXLEN: z.coerce.number().int().positive().default(10_000),
});

export type GatewayConfig = z.infer<typeof schema>;

export const loadGatewayConfig = (env: NodeJS.ProcessEnv = process.env): GatewayConfig => schema.parse(env);
