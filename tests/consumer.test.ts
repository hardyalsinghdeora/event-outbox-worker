import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryBroker } from '../src/broker/inMemory.js';
import type { Delivery } from '../src/broker/port.js';
import { Consumer, type ConsumerOptions, type EventHandler } from '../src/consumer/consumer.js';
import { INVENTORY_PROJECTION, inventoryProjection, readInventoryProjection } from '../src/consumer/inventoryProjection.js';
import { listDeadLetters } from '../src/deadLetters.js';
import type { EventEnvelope } from '../src/events.js';
import { count, fastBackoff, manualClock, openTempDatabase, orderPlacedEnvelope, silentLogger, type TempDatabase } from './helpers.js';

const deliveryOf = (envelope: EventEnvelope, attempt = 1): Delivery => ({
  message: { id: envelope.id, topic: envelope.type, body: JSON.stringify(envelope) },
  attempt,
});

describe('Consumer', () => {
  let tmp: TempDatabase;
  const clock = manualClock();
  beforeEach(() => {
    tmp = openTempDatabase();
  });
  afterEach(() => {
    tmp.cleanup();
  });

  const consumerWith = (overrides: Partial<ConsumerOptions> = {}) =>
    new Consumer({
      name: INVENTORY_PROJECTION,
      db: tmp.db,
      broker: new InMemoryBroker(),
      handler: inventoryProjection,
      logger: silentLogger,
      maxAttempts: 3,
      backoff: fastBackoff,
      clock,
      ...overrides,
    });

  it('applies a duplicate delivery exactly once', async () => {
    const consumer = consumerWith();
    const envelope = orderPlacedEnvelope({}, [
      { sku: 'SKU-A', qty: 2, unitPriceCents: 100 },
      { sku: 'SKU-B', qty: 5, unitPriceCents: 100 },
    ]);

    expect(await consumer.handle(deliveryOf(envelope))).toEqual({ kind: 'ack' });
    expect(await consumer.handle(deliveryOf(envelope, 2))).toEqual({ kind: 'ack' });

    expect(readInventoryProjection(tmp.db)).toEqual([
      { sku: 'SKU-A', reserved_qty: 2 },
      { sku: 'SKU-B', reserved_qty: 5 },
    ]);
    expect(tmp.db.all('SELECT consumer, event_id FROM processed_events')).toEqual([{ consumer: INVENTORY_PROJECTION, event_id: envelope.id }]);
    expect(consumer.snapshot()).toEqual({ processed: 1, duplicates: 1, failed: 0, deadLettered: 0 });
  });

  it('keeps one effect under concurrent deliveries of the same event', async () => {
    const consumer = consumerWith();
    const envelope = orderPlacedEnvelope({}, [{ sku: 'SKU-A', qty: 3, unitPriceCents: 100 }]);

    const outcomes = await Promise.all([1, 2, 3].map((attempt) => consumer.handle(deliveryOf(envelope, attempt))));
    expect(outcomes).toEqual([{ kind: 'ack' }, { kind: 'ack' }, { kind: 'ack' }]);
    expect(readInventoryProjection(tmp.db)).toEqual([{ sku: 'SKU-A', reserved_qty: 3 }]);
    expect(count(tmp.db, 'processed_events')).toBe(1);
    expect(consumer.snapshot()).toMatchObject({ processed: 1, duplicates: 2 });
  });

  it('rolls back the effect and does not mark the event when the handler throws', async () => {
    const halfway: EventHandler = {
      eventType: inventoryProjection.eventType,
      apply(db, event) {
        inventoryProjection.apply(db, event);
        throw new Error('downstream exploded');
      },
    };
    const consumer = consumerWith({ handler: halfway });
    const outcome = await consumer.handle(deliveryOf(orderPlacedEnvelope()));

    expect(outcome).toMatchObject({ kind: 'nack' });
    expect(outcome.kind === 'nack' ? outcome.retryAfterMs : -1).toBeGreaterThanOrEqual(fastBackoff.baseDelayMs / 2);
    expect(readInventoryProjection(tmp.db)).toEqual([]);
    expect(count(tmp.db, 'processed_events')).toBe(0);
    expect(consumer.snapshot()).toMatchObject({ processed: 0, failed: 1 });
  });

  it('dead-letters a poison payload after maxAttempts and acks it away', async () => {
    const consumer = consumerWith({ maxAttempts: 3 });
    const poison = orderPlacedEnvelope({}, [{ sku: 'SKU-A', qty: 0, unitPriceCents: 100 }]);

    expect(await consumer.handle(deliveryOf(poison, 1))).toMatchObject({ kind: 'nack' });
    expect(await consumer.handle(deliveryOf(poison, 2))).toMatchObject({ kind: 'nack' });
    expect(listDeadLetters(tmp.db)).toEqual([]);
    expect(await consumer.handle(deliveryOf(poison, 3))).toEqual({ kind: 'ack' });

    expect(listDeadLetters(tmp.db)).toMatchObject([
      { consumer: INVENTORY_PROJECTION, event_id: poison.id, error: 'ZodError: lines.0.qty: Too small: expected number to be >0', failed_at: clock().toISOString() },
    ]);
    expect(readInventoryProjection(tmp.db)).toEqual([]);
    expect(count(tmp.db, 'processed_events')).toBe(0);
    expect(consumer.snapshot()).toEqual({ processed: 0, duplicates: 0, failed: 2, deadLettered: 1 });
  });

  it('dead-letters an undecodable message the same way', async () => {
    const consumer = consumerWith({ maxAttempts: 1 });
    expect(await consumer.handle({ message: { id: 'garbage', topic: 'OrderPlaced', body: '{"nope":true}' }, attempt: 1 })).toEqual({ kind: 'ack' });
    expect(listDeadLetters(tmp.db)).toMatchObject([{ event_id: 'garbage', payload: '{"nope":true}' }]);
  });

  it('subscribes through the broker: nack, redelivery with backoff, then dead letter', async () => {
    const broker = new InMemoryBroker();
    const consumer = consumerWith({ broker, maxAttempts: 2, clock: () => new Date() });
    consumer.start();
    const good = orderPlacedEnvelope({}, [{ sku: 'SKU-Z', qty: 1, unitPriceCents: 100 }]);
    const poison = orderPlacedEnvelope({}, [{ sku: 'SKU-Z', qty: -1, unitPriceCents: 100 }]);
    await broker.publish(deliveryOf(poison).message);
    await broker.publish(deliveryOf(good).message);

    await vi.waitUntil(() => consumer.snapshot().deadLettered === 1 && consumer.snapshot().processed === 1, { timeout: 2_000 });
    await consumer.stop();
    expect(consumer.snapshot()).toEqual({ processed: 1, duplicates: 0, failed: 1, deadLettered: 1 });
    expect(readInventoryProjection(tmp.db)).toEqual([{ sku: 'SKU-Z', reserved_qty: 1 }]);
  });
});
