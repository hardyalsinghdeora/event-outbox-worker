import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryBroker, type InMemoryBrokerOptions, type PublishFault } from '../src/broker/inMemory.js';
import type { Message } from '../src/broker/port.js';
import { listDeadLetters } from '../src/deadLetters.js';
import { ORDER_PLACED } from '../src/events.js';
import { readHealth } from '../src/health.js';
import { placeOrder } from '../src/orders/repository.js';
import { OutboxRelay, RELAY_NAME, type RelayOptions } from '../src/outbox/relay.js';
import { appendOutboxEvent, type OutboxRow } from '../src/outbox/repository.js';
import { fastBackoff, manualClock, openTempDatabase, orderPlacedEnvelope, silentLogger, type TempDatabase } from './helpers.js';

describe('OutboxRelay', () => {
  let tmp: TempDatabase;
  const clock = manualClock();
  beforeEach(() => {
    tmp = openTempDatabase();
  });
  afterEach(() => {
    tmp.cleanup();
  });

  const relayWith = (broker: InMemoryBroker, overrides: Partial<RelayOptions> = {}) =>
    new OutboxRelay({ db: tmp.db, broker, logger: silentLogger, batchSize: 10, maxAttempts: 3, pollIntervalMs: 5, backoff: fastBackoff, clock, ...overrides });

  const appendEvent = (aggregateId: string): string => {
    const envelope = orderPlacedEnvelope({ aggregateId });
    appendOutboxEvent(tmp.db, { aggregateType: 'order', envelope, now: clock().toISOString() });
    return envelope.id;
  };

  const outboxRows = () => tmp.db.all<OutboxRow>('SELECT * FROM outbox ORDER BY id');
  const publishedIds = (broker: InMemoryBroker) => broker.published(ORDER_PLACED).map((m) => m.id);
  const faultFor = (decide: (message: Message, attempt: number) => PublishFault | undefined): InMemoryBrokerOptions => ({ injectPublishFault: decide });

  it('publishes due rows in id order and marks them published only after the ack', async () => {
    const ids = [appendEvent('a'), appendEvent('b'), appendEvent('c')];
    const broker = new InMemoryBroker();
    const relay = relayWith(broker);

    expect(await relay.runOnce()).toBe(3);
    expect(publishedIds(broker)).toEqual(ids);
    expect(outboxRows().map((row) => row.published_at)).toEqual([clock().toISOString(), clock().toISOString(), clock().toISOString()]);
    expect(await relay.runOnce()).toBe(0);
    expect(relay.snapshot()).toEqual({ published: 3, failed: 0, deadLettered: 0 });
  });

  it('records attempts, error and backoff on failure, then retries once due', async () => {
    appendEvent('a');
    const broker = new InMemoryBroker(faultFor((_m, attempt) => (attempt === 1 ? 'reject' : undefined)));
    const relay = relayWith(broker);

    await relay.runOnce();
    const [row] = outboxRows();
    expect(row).toMatchObject({ attempts: 1, published_at: null, last_error: 'Error: injected publish failure (attempt 1)' });
    const delay = Date.parse(row?.next_attempt_at ?? '') - clock().getTime();
    expect(delay).toBeGreaterThanOrEqual(fastBackoff.baseDelayMs / 2);
    expect(delay).toBeLessThanOrEqual(fastBackoff.baseDelayMs);
    expect(publishedIds(broker)).toEqual([]);

    expect(await relay.runOnce()).toBe(0); // not due yet
    clock.advance(fastBackoff.maxDelayMs);
    expect(await relay.runOnce()).toBe(1);
    expect(outboxRows()[0]?.published_at).not.toBeNull();
    expect(publishedIds(broker)).toHaveLength(1);
    expect(relay.snapshot()).toEqual({ published: 1, failed: 1, deadLettered: 0 });
  });

  it('keeps a row unpublished when the ack is lost, so a retry produces a duplicate rather than a loss', async () => {
    const id = appendEvent('a');
    const broker = new InMemoryBroker(faultFor((_m, attempt) => (attempt === 1 ? 'lost-ack' : undefined)));
    const relay = relayWith(broker);

    await relay.runOnce();
    expect(publishedIds(broker)).toEqual([id]);
    expect(outboxRows()[0]).toMatchObject({ attempts: 1, published_at: null });

    clock.advance(fastBackoff.maxDelayMs);
    await relay.runOnce();
    expect(publishedIds(broker)).toEqual([id, id]);
    expect(outboxRows()[0]?.published_at).not.toBeNull();
  });

  it('dead-letters a row after maxAttempts and takes it out of the backlog', async () => {
    const id = appendEvent('a');
    const broker = new InMemoryBroker(faultFor(() => 'reject'));
    const relay = relayWith(broker, { maxAttempts: 2 });

    await relay.runOnce();
    clock.advance(fastBackoff.maxDelayMs);
    await relay.runOnce();

    expect(listDeadLetters(tmp.db)).toMatchObject([{ consumer: RELAY_NAME, event_id: id, error: 'Error: injected publish failure (attempt 2)' }]);
    expect(outboxRows()[0]).toMatchObject({ attempts: 2, published_at: clock().toISOString() });
    expect(readHealth(tmp.db, clock)).toMatchObject({ outbox: { backlog: 0 }, deadLetters: 1 });
    expect(relay.snapshot()).toEqual({ published: 0, failed: 1, deadLettered: 1 });
    expect(await relay.runOnce()).toBe(0);
  });

  it('resumes exactly from the unpublished rows after a restart', async () => {
    const ids = ['a', 'b', 'c', 'd'].map(appendEvent);
    const broker = new InMemoryBroker();

    expect(await relayWith(broker, { batchSize: 2 }).runOnce()).toBe(2);
    expect(publishedIds(broker)).toEqual(ids.slice(0, 2));

    const restarted = relayWith(broker, { batchSize: 10 });
    expect(await restarted.runOnce()).toBe(2);
    expect(publishedIds(broker)).toEqual(ids);
    expect(await restarted.runOnce()).toBe(0);
  });

  it('lets a stuck aggregate stall only itself, keeping its own rows in order', async () => {
    const a1 = appendEvent('A');
    const b1 = appendEvent('B');
    const a2 = appendEvent('A');
    let aFails = true;
    const broker = new InMemoryBroker(faultFor((message) => (aFails && message.id === a1 ? 'reject' : undefined)));
    const relay = relayWith(broker, { maxAttempts: 10 });

    await relay.runOnce();
    expect(publishedIds(broker)).toEqual([b1]);
    clock.advance(fastBackoff.maxDelayMs);
    await relay.runOnce();
    expect(publishedIds(broker)).toEqual([b1]); // a2 waits behind a1 rather than overtaking it
    expect(outboxRows().map((row) => row.attempts)).toEqual([2, 0, 0]);

    aFails = false;
    clock.advance(fastBackoff.maxDelayMs);
    await relay.runOnce();
    await relay.runOnce();
    expect(publishedIds(broker)).toEqual([b1, a1, a2]);
  });

  it('runs as a background loop and stops after the batch in progress', async () => {
    const broker = new InMemoryBroker();
    const relay = relayWith(broker, { clock: () => new Date() });
    relay.start();

    placeOrder(tmp.db, { customerId: 'c', lines: [{ sku: 'SKU-A', qty: 1, unitPriceCents: 1 }] }, { clock: () => new Date() });
    await vi.waitUntil(() => publishedIds(broker).length === 1, { timeout: 2_000, interval: 5 });

    await relay.stop();
    placeOrder(tmp.db, { customerId: 'c', lines: [{ sku: 'SKU-A', qty: 1, unitPriceCents: 1 }] }, { clock: () => new Date() });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(publishedIds(broker)).toHaveLength(1);
    expect(readHealth(tmp.db, () => new Date()).outbox.backlog).toBe(1);
  });
});
