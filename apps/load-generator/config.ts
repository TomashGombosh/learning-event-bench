import { z } from 'zod';

const schema = z.object({
  GATEWAY_URLS: z.string().default('http://localhost:3000'),
  RUN_ID: z.string().default(() => `run-${Date.now()}`),
  SCHEME: z.enum(['yjs', 'sharedb']).default('yjs'),
  BROKER_LABEL: z.string().default('unknown'),
  ROOMS: z.coerce.number().int().positive().default(5),
  CLIENTS_PER_ROOM: z.coerce.number().int().min(2).default(5),
  RATE_PER_CLIENT: z.coerce.number().positive().default(5),
  WARMUP_S: z.coerce.number().min(0).default(5),
  DURATION_S: z.coerce.number().positive().default(30),
  DRAIN_S: z.coerce.number().min(0).default(5),
  DISCONNECTS_PER_MIN: z.coerce.number().min(0).default(0),
  MEAN_OFFLINE_S: z.coerce.number().positive().default(2),
  SEED: z.coerce.number().int().default(42),
  RESERVOIR: z.coerce.number().int().positive().default(50_000),
  SHARD_INDEX: z.coerce.number().int().min(0).default(0),
  SHARD_COUNT: z.coerce.number().int().positive().default(1),
  OUT_DIR: z.string().default('results'),
});

export type LoadConfig = z.infer<typeof schema>;

export const loadConfig = (env: NodeJS.ProcessEnv = process.env): LoadConfig => schema.parse(env);
