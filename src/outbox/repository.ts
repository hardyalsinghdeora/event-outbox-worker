import type { Database } from '../db/database.js';
import type { EventEnvelope } from '../events.js';

export interface OutboxRow {
  id: number;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  payload: string;
  created_at: string;
  published_at: string | null;
  attempts: number;
  next_attempt_at: string;
  last_error: string | null;
}

export interface AppendOutboxInput {
  aggregateType: string;
  envelope: EventEnvelope;
  now: string;
}

/** Call this inside the transaction that persists the aggregate change; that co-location is the whole pattern. */
export function appendOutboxEvent(db: Database, { aggregateType, envelope, now }: AppendOutboxInput): number {
  const result = db.run(
    `INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, created_at, next_attempt_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [aggregateType, envelope.aggregateId, envelope.type, JSON.stringify(envelope), now, now],
  );
  return result.lastInsertRowid;
}

/**
 * The oldest unpublished row of each aggregate, when it is due, in id order. Later rows of the same
 * aggregate wait behind it, so a failing aggregate stalls only itself while others keep flowing.
 */
export function selectDueOutboxHeads(db: Database, now: string, limit: number): OutboxRow[] {
  return db.all<OutboxRow>(
    `SELECT o.* FROM outbox o
     WHERE o.published_at IS NULL
       AND o.next_attempt_at <= ?
       AND NOT EXISTS (
         SELECT 1 FROM outbox p
         WHERE p.published_at IS NULL
           AND p.aggregate_type = o.aggregate_type
           AND p.aggregate_id = o.aggregate_id
           AND p.id < o.id)
     ORDER BY o.id
     LIMIT ?`,
    [now, limit],
  );
}

export function markOutboxPublished(db: Database, id: number, now: string): void {
  db.run('UPDATE outbox SET published_at = ? WHERE id = ?', [now, id]);
}

export interface OutboxFailure {
  attempts: number;
  nextAttemptAt: string;
  error: string;
}

export function recordOutboxFailure(db: Database, id: number, { attempts, nextAttemptAt, error }: OutboxFailure): void {
  db.run('UPDATE outbox SET attempts = ?, next_attempt_at = ?, last_error = ? WHERE id = ?', [attempts, nextAttemptAt, error, id]);
}

/** Takes a row out of the unpublished set after it was dead-lettered, so it no longer blocks its aggregate. */
export function abandonOutboxRow(db: Database, id: number, attempts: number, error: string, now: string): void {
  db.run('UPDATE outbox SET published_at = ?, attempts = ?, last_error = ? WHERE id = ?', [now, attempts, error, id]);
}

export interface OutboxBacklog {
  backlog: number;
  oldestUnpublishedAgeMs: number | null;
}

export function readOutboxBacklog(db: Database, now: Date): OutboxBacklog {
  const row = db.get<{ backlog: number; oldest: string | null }>(
    'SELECT COUNT(*) AS backlog, MIN(created_at) AS oldest FROM outbox WHERE published_at IS NULL',
  );
  return {
    backlog: row?.backlog ?? 0,
    oldestUnpublishedAgeMs: row?.oldest ? Math.max(0, now.getTime() - Date.parse(row.oldest)) : null,
  };
}
