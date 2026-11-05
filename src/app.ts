import { serve, type ServerType } from '@hono/node-server';
import type { Hono } from 'hono';
import type { Broker } from './broker/port.js';
import { type Clock, systemClock } from './clock.js';
import type { Config } from './config.js';
import { Consumer } from './consumer/consumer.js';
import { INVENTORY_PROJECTION, inventoryProjection } from './consumer/inventoryProjection.js';
import type { Database } from './db/database.js';
import { migrate } from './db/migrate.js';
import type { Logger } from './logger.js';
import { createApi } from './orders/api.js';
import { OutboxRelay } from './outbox/relay.js';

export interface AppDeps {
  config: Config;
  db: Database;
  broker: Broker;
  logger: Logger;
  clock?: Clock;
}

export interface App {
  api: Hono;
  relay: OutboxRelay;
  consumer: Consumer;
  /** Starts HTTP, consumer and relay (in that order, so deliveries always have a listener); resolves with the bound port. */
  start(port: number): Promise<number>;
  /** Stops accepting requests, lets the relay batch and handler in flight finish, then closes the database. */
  stop(): Promise<void>;
}

/** One process hosting all three roles. Each depends only on `Database` and `Broker`, so they could be split. */
export function createApp({ config, db, broker, logger, clock = systemClock }: AppDeps): App {
  migrate(db, { logger, clock });
  const api = createApi({ db, logger, clock });
  const backoff = { baseDelayMs: config.backoffBaseMs, maxDelayMs: config.backoffMaxMs };
  const relay = new OutboxRelay({
    db,
    broker,
    logger,
    clock,
    batchSize: config.relayBatchSize,
    maxAttempts: config.maxAttempts,
    pollIntervalMs: config.relayPollIntervalMs,
    backoff,
  });
  const consumer = new Consumer({
    name: INVENTORY_PROJECTION,
    db,
    broker,
    handler: inventoryProjection,
    logger,
    clock,
    maxAttempts: config.maxAttempts,
    backoff,
  });
  let server: ServerType | undefined;

  return {
    api,
    relay,
    consumer,
    start: (port) =>
      new Promise<number>((resolve) => {
        server = serve({ fetch: api.fetch, port }, (info) => {
          consumer.start();
          relay.start();
          logger.info({ port: info.port }, 'listening');
          resolve(info.port);
        });
      }),
    stop: async () => {
      await new Promise<void>((resolve, reject) => {
        if (!server) {
          resolve();
          return;
        }
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
      await relay.stop();
      await consumer.stop();
      db.close();
      logger.info('stopped');
    },
  };
}
