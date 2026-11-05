import { type Clock, isoAfter, isoNow, systemClock } from '../clock.js';
import type { Database } from '../db/database.js';
import { createPollLoop } from '../pollLoop.js';
import { type Broker, type Message, type MessageHandler, runHandler, type Subscription } from './port.js';

export interface SqliteQueueBrokerOptions {
  pollIntervalMs?: number;
  /** How long a delivered message stays invisible to other consumers before it is retried. */
  leaseMs?: number;
  clock?: Clock;
  onError?: (error: unknown) => void;
}

interface QueueRow {
  id: number;
  message_id: string;
  payload: string;
  attempts: number;
}

/**
 * A queue table as the broker: durable, drainable by another process on the same database file,
 * with SQS-style visibility timeouts. Enough to exercise the relay/consumer contract; not a message broker.
 */
export class SqliteQueueBroker implements Broker {
  private readonly pollIntervalMs: number;
  private readonly leaseMs: number;
  private readonly clock: Clock;
  private readonly onError: (error: unknown) => void;

  constructor(
    private readonly db: Database,
    options: SqliteQueueBrokerOptions = {},
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 250;
    this.leaseMs = options.leaseMs ?? 30_000;
    this.clock = options.clock ?? systemClock;
    this.onError = options.onError ?? (() => undefined);
  }

  publish(message: Message): Promise<void> {
    const now = isoNow(this.clock);
    this.db.run('INSERT INTO queue (topic, message_id, payload, enqueued_at, visible_at) VALUES (?, ?, ?, ?, ?)', [
      message.topic,
      message.id,
      message.body,
      now,
      now,
    ]);
    return Promise.resolve();
  }

  subscribe(topic: string, handler: MessageHandler): Subscription {
    const loop = createPollLoop({
      tick: () => this.deliverNext(topic, handler),
      idleDelayMs: this.pollIntervalMs,
      onError: this.onError,
    });
    loop.start();
    return { close: () => loop.stop() };
  }

  /** Leases the oldest visible message, runs the handler, then deletes it (ack) or hides it until the retry time (nack). */
  private async deliverNext(topic: string, handler: MessageHandler): Promise<boolean> {
    const row = this.lease(topic);
    if (!row) return false;
    const delivery = { message: { id: row.message_id, topic, body: row.payload }, attempt: row.attempts };
    const outcome = await runHandler(handler, delivery, this.onError);
    if (outcome.kind === 'ack') {
      this.db.run('DELETE FROM queue WHERE id = ?', [row.id]);
    } else {
      this.db.run('UPDATE queue SET visible_at = ? WHERE id = ?', [isoAfter(this.clock, outcome.retryAfterMs), row.id]);
    }
    return true;
  }

  /** The write lock taken by BEGIN IMMEDIATE is what `SELECT … FOR UPDATE SKIP LOCKED` gives you in Postgres. */
  private lease(topic: string): QueueRow | undefined {
    return this.db.transaction(() => {
      const row = this.db.get<QueueRow>(
        'SELECT id, message_id, payload, attempts FROM queue WHERE topic = ? AND visible_at <= ? ORDER BY id LIMIT 1',
        [topic, isoNow(this.clock)],
      );
      if (!row) return undefined;
      const attempts = row.attempts + 1;
      this.db.run('UPDATE queue SET attempts = ?, visible_at = ? WHERE id = ?', [attempts, isoAfter(this.clock, this.leaseMs), row.id]);
      return { ...row, attempts };
    });
  }
}
