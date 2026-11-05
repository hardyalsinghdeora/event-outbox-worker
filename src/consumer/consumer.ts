import type { Broker, Delivery, Outcome, Subscription } from '../broker/port.js';
import { type Clock, isoNow, systemClock } from '../clock.js';
import type { Database } from '../db/database.js';
import { recordDeadLetter } from '../deadLetters.js';
import { describeError } from '../errors.js';
import { type EventEnvelope, parseEnvelope } from '../events.js';
import type { Logger } from '../logger.js';
import { type BackoffPolicy, backoffDelayMs } from '../outbox/backoff.js';

/** What a consumer does with one event. `apply` runs inside the consumer's transaction and must be synchronous. */
export interface EventHandler {
  eventType: string;
  apply(db: Database, event: EventEnvelope): void;
}

export interface ConsumerOptions {
  name: string;
  db: Database;
  broker: Broker;
  handler: EventHandler;
  logger: Logger;
  maxAttempts: number;
  backoff: BackoffPolicy;
  clock?: Clock;
}

export interface ConsumerStats {
  processed: number;
  duplicates: number;
  failed: number;
  deadLettered: number;
}

/**
 * Idempotent consumer: the processed_events check, the handler's effect and the processed_events
 * insert commit together, so a redelivered event has exactly one effect (ADR 0002). A failing
 * handler is nacked with backoff; after maxAttempts the event is dead-lettered and acked (ADR 0003).
 */
export class Consumer {
  private readonly stats: ConsumerStats = { processed: 0, duplicates: 0, failed: 0, deadLettered: 0 };
  private readonly clock: Clock;
  private readonly log: Logger;
  private subscription: Subscription | undefined;

  constructor(private readonly options: ConsumerOptions) {
    this.clock = options.clock ?? systemClock;
    this.log = options.logger.child({ component: options.name });
  }

  start(): void {
    this.subscription ??= this.options.broker.subscribe(this.options.handler.eventType, (delivery) => this.handle(delivery));
  }

  async stop(): Promise<void> {
    await this.subscription?.close();
    this.subscription = undefined;
  }

  snapshot(): ConsumerStats {
    return { ...this.stats };
  }

  handle(delivery: Delivery): Promise<Outcome> {
    return Promise.resolve(this.process(delivery));
  }

  private process(delivery: Delivery): Outcome {
    const { db, name, handler, maxAttempts, backoff } = this.options;
    const log = this.log.child({ eventId: delivery.message.id, attempt: delivery.attempt });
    try {
      const event = parseEnvelope(delivery.message.body);
      const result = db.transaction((): 'processed' | 'duplicate' => {
        if (isProcessed(db, name, event.id)) return 'duplicate';
        handler.apply(db, event);
        markProcessed(db, name, event.id, isoNow(this.clock));
        return 'processed';
      });
      if (result === 'duplicate') {
        this.stats.duplicates += 1;
        log.info({ aggregateId: event.aggregateId }, 'duplicate delivery suppressed');
      } else {
        this.stats.processed += 1;
        log.info({ aggregateId: event.aggregateId }, 'event processed');
      }
      return { kind: 'ack' };
    } catch (error) {
      const reason = describeError(error);
      if (delivery.attempt >= maxAttempts) {
        recordDeadLetter(db, {
          consumer: name,
          eventId: delivery.message.id,
          payload: delivery.message.body,
          error: reason,
          now: isoNow(this.clock),
        });
        this.stats.deadLettered += 1;
        log.error({ reason }, 'event dead-lettered after max attempts');
        // Acked on purpose: the dead letter is now the record, and the queue must not keep redelivering it.
        return { kind: 'ack' };
      }
      const retryAfterMs = backoffDelayMs(delivery.attempt, backoff);
      this.stats.failed += 1;
      log.warn({ reason, retryAfterMs }, 'handler failed, nacked for redelivery');
      return { kind: 'nack', retryAfterMs };
    }
  }
}

function isProcessed(db: Database, consumer: string, eventId: string): boolean {
  return db.get('SELECT 1 AS seen FROM processed_events WHERE consumer = ? AND event_id = ?', [consumer, eventId]) !== undefined;
}

function markProcessed(db: Database, consumer: string, eventId: string, now: string): void {
  db.run('INSERT INTO processed_events (consumer, event_id, processed_at) VALUES (?, ?, ?)', [consumer, eventId, now]);
}
