import type { Broker } from '../broker/port.js';
import { type Clock, isoAfter, isoNow, systemClock } from '../clock.js';
import type { Database } from '../db/database.js';
import { recordDeadLetter } from '../deadLetters.js';
import { describeError } from '../errors.js';
import { tryParseEnvelope } from '../events.js';
import type { Logger } from '../logger.js';
import { createPollLoop, type PollLoop } from '../pollLoop.js';
import { type BackoffPolicy, backoffDelayMs } from './backoff.js';
import {
  abandonOutboxRow,
  markOutboxPublished,
  type OutboxRow,
  recordOutboxFailure,
  selectDueOutboxHeads,
} from './repository.js';

export const RELAY_NAME = 'outbox-relay';

export interface RelayOptions {
  db: Database;
  broker: Broker;
  logger: Logger;
  batchSize: number;
  maxAttempts: number;
  pollIntervalMs: number;
  backoff: BackoffPolicy;
  clock?: Clock;
}

export interface RelayStats {
  published: number;
  failed: number;
  deadLettered: number;
}

/**
 * Moves outbox rows to the broker at least once, in id order per aggregate. A row is marked
 * published only after the broker acknowledges it, so a crash in between costs a duplicate,
 * never a lost event (ADR 0001).
 */
export class OutboxRelay {
  private readonly stats: RelayStats = { published: 0, failed: 0, deadLettered: 0 };
  private readonly loop: PollLoop;
  private readonly clock: Clock;
  private readonly log: Logger;

  constructor(private readonly options: RelayOptions) {
    this.clock = options.clock ?? systemClock;
    this.log = options.logger.child({ component: RELAY_NAME });
    this.loop = createPollLoop({
      tick: async () => (await this.runOnce()) > 0,
      idleDelayMs: options.pollIntervalMs,
      onError: (error) => {
        this.log.error({ err: error }, 'relay batch failed');
      },
    });
  }

  start(): void {
    this.loop.start();
  }

  stop(): Promise<void> {
    return this.loop.stop();
  }

  snapshot(): RelayStats {
    return { ...this.stats };
  }

  /** Publishes one batch of due rows in id order; returns how many rows it attempted. */
  async runOnce(): Promise<number> {
    const rows = selectDueOutboxHeads(this.options.db, isoNow(this.clock), this.options.batchSize);
    for (const row of rows) await this.publishRow(row);
    return rows.length;
  }

  private async publishRow(row: OutboxRow): Promise<void> {
    const { db, broker, maxAttempts, backoff } = this.options;
    const envelope = tryParseEnvelope(row.payload);
    const eventId = envelope?.id ?? `outbox:${String(row.id)}`;
    const log = this.log.child({ outboxId: row.id, eventId, aggregateId: row.aggregate_id, eventType: row.event_type });
    const attempts = row.attempts + 1;

    try {
      if (!envelope) throw new Error('payload is not a valid event envelope');
      await broker.publish({ id: envelope.id, topic: row.event_type, body: row.payload });
    } catch (error) {
      const reason = describeError(error);
      if (attempts >= maxAttempts) {
        const now = isoNow(this.clock);
        db.transaction(() => {
          recordDeadLetter(db, { consumer: RELAY_NAME, eventId, payload: row.payload, error: reason, now });
          abandonOutboxRow(db, row.id, attempts, reason, now);
        });
        this.stats.deadLettered += 1;
        log.error({ attempts, reason }, 'event dead-lettered after max publish attempts');
        return;
      }
      const nextAttemptAt = isoAfter(this.clock, backoffDelayMs(attempts, backoff));
      recordOutboxFailure(db, row.id, { attempts, nextAttemptAt, error: reason });
      this.stats.failed += 1;
      log.warn({ attempts, nextAttemptAt, reason }, 'publish failed, will retry');
      return;
    }

    markOutboxPublished(db, row.id, isoNow(this.clock));
    this.stats.published += 1;
    log.info({ attempts }, 'event published');
  }
}
