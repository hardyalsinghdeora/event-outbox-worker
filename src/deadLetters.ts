import type { Database } from './db/database.js';

export interface DeadLetter {
  consumer: string;
  event_id: string;
  payload: string;
  error: string;
  failed_at: string;
}

export interface DeadLetterInput {
  consumer: string;
  eventId: string;
  payload: string;
  error: string;
  now: string;
}

/** Upsert: a replayed dead letter that fails again keeps one row carrying the latest error. */
export function recordDeadLetter(db: Database, { consumer, eventId, payload, error, now }: DeadLetterInput): void {
  db.run(
    `INSERT INTO dead_letters (consumer, event_id, payload, error, failed_at) VALUES (?, ?, ?, ?, ?)
     ON CONFLICT (consumer, event_id) DO UPDATE SET payload = excluded.payload, error = excluded.error, failed_at = excluded.failed_at`,
    [consumer, eventId, payload, error, now],
  );
}

export function listDeadLetters(db: Database): DeadLetter[] {
  return db.all<DeadLetter>('SELECT consumer, event_id, payload, error, failed_at FROM dead_letters ORDER BY failed_at, event_id');
}

export function countDeadLetters(db: Database): number {
  return db.get<{ count: number }>('SELECT COUNT(*) AS count FROM dead_letters')?.count ?? 0;
}
