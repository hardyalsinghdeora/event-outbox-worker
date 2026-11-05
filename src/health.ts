import type { Clock } from './clock.js';
import type { Database } from './db/database.js';
import { countDeadLetters } from './deadLetters.js';
import { type OutboxBacklog, readOutboxBacklog } from './outbox/repository.js';

export interface Health {
  status: 'ok';
  outbox: OutboxBacklog;
  deadLetters: number;
}

export function readHealth(db: Database, clock: Clock): Health {
  return { status: 'ok', outbox: readOutboxBacklog(db, clock()), deadLetters: countDeadLetters(db) };
}
