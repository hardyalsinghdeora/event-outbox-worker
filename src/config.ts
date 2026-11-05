import { z } from 'zod';

const LOG_LEVELS = ['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'] as const;

const envSchema = z.object({
  PORT: z.coerce.number().int().min(0).max(65535).default(3000),
  DB_PATH: z.string().min(1).default('./data/outbox.db'),
  LOG_LEVEL: z.enum(LOG_LEVELS).default('info'),
  RELAY_BATCH_SIZE: z.coerce.number().int().positive().default(50),
  MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  RELAY_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(500),
  BACKOFF_BASE_MS: z.coerce.number().int().positive().default(500),
  BACKOFF_MAX_MS: z.coerce.number().int().positive().default(30_000),
});

export interface Config {
  port: number;
  dbPath: string;
  logLevel: (typeof LOG_LEVELS)[number];
  relayBatchSize: number;
  maxAttempts: number;
  relayPollIntervalMs: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = envSchema.safeParse(env);
  if (!parsed.success) {
    throw new Error(`Invalid configuration: ${z.prettifyError(parsed.error)}`);
  }
  const e = parsed.data;
  return {
    port: e.PORT,
    dbPath: e.DB_PATH,
    logLevel: e.LOG_LEVEL,
    relayBatchSize: e.RELAY_BATCH_SIZE,
    maxAttempts: e.MAX_ATTEMPTS,
    relayPollIntervalMs: e.RELAY_POLL_INTERVAL_MS,
    backoffBaseMs: e.BACKOFF_BASE_MS,
    backoffMaxMs: e.BACKOFF_MAX_MS,
  };
}
