import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pino } from 'pino';
import type { Clock } from '../src/clock.js';
import type { Config } from '../src/config.js';
import { type Database, openDatabase, type SqlValue } from '../src/db/database.js';
import { migrate } from '../src/db/migrate.js';
import { type EventEnvelope, ORDER_PLACED, type OrderPlaced } from '../src/events.js';
import type { Logger } from '../src/logger.js';
import type { BackoffPolicy } from '../src/outbox/backoff.js';

export const silentLogger: Logger = pino({ level: 'silent' });

export const fastBackoff: BackoffPolicy = { baseDelayMs: 10, maxDelayMs: 40 };

export interface TempDatabase {
  db: Database;
  path: string;
  cleanup(): void;
}

/** A migrated SQLite file in a fresh temp directory; cleanup tolerates a database the test already closed. */
export function openTempDatabase(): TempDatabase {
  const dir = mkdtempSync(join(tmpdir(), 'outbox-test-'));
  const path = join(dir, 'test.db');
  const db = openDatabase(path);
  migrate(db);
  return {
    db,
    path,
    cleanup: () => {
      try {
        db.close();
      } catch {
        // already closed by the code under test
      }
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

export function testConfig(overrides: Partial<Config> = {}): Config {
  return {
    port: 0,
    dbPath: ':memory:',
    logLevel: 'silent',
    relayBatchSize: 10,
    maxAttempts: 3,
    relayPollIntervalMs: 10,
    backoffBaseMs: fastBackoff.baseDelayMs,
    backoffMaxMs: fastBackoff.maxDelayMs,
    ...overrides,
  };
}

/** A clock the test moves by hand. */
export function manualClock(start = '2026-01-01T00:00:00.000Z'): Clock & { advance(ms: number): void } {
  let now = new Date(start);
  const clock = (): Date => now;
  return Object.assign(clock, {
    advance(ms: number) {
      now = new Date(now.getTime() + ms);
    },
  });
}

export function orderPlacedEnvelope(overrides: Partial<EventEnvelope<OrderPlaced>> = {}, lines = [{ sku: 'SKU-1', qty: 2, unitPriceCents: 100 }]): EventEnvelope<OrderPlaced> {
  const orderId = overrides.aggregateId ?? crypto.randomUUID();
  return {
    id: crypto.randomUUID(),
    type: ORDER_PLACED,
    occurredAt: '2026-01-01T00:00:00.000Z',
    aggregateId: orderId,
    data: { orderId, customerId: 'cust-1', totalCents: lines.reduce((sum, l) => sum + l.qty * l.unitPriceCents, 0), lines },
    ...overrides,
  };
}

/** Wraps a database so statements matching `pattern` fail; everything else, including transactions, is real. */
export function failingOn(db: Database, pattern: string): Database {
  return {
    exec: (sql) => {
      db.exec(sql);
    },
    run: (sql: string, params?: SqlValue[]) => {
      if (sql.includes(pattern)) throw new Error(`injected failure: ${pattern}`);
      return db.run(sql, params);
    },
    get: <T extends object>(sql: string, params?: SqlValue[]) => db.get<T>(sql, params),
    all: <T extends object>(sql: string, params?: SqlValue[]) => db.all<T>(sql, params),
    transaction: <T>(fn: () => T) => db.transaction(fn),
    close: () => {
      db.close();
    },
  };
}

export function count(db: Database, table: string): number {
  return db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`)?.n ?? 0;
}
