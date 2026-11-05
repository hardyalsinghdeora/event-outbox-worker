import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { type App, createApp } from '../src/app.js';
import { InMemoryBroker } from '../src/broker/inMemory.js';
import { SqliteQueueBroker } from '../src/broker/sqliteQueue.js';
import { readInventoryProjection } from '../src/consumer/inventoryProjection.js';
import { ORDER_PLACED } from '../src/events.js';
import type { Health } from '../src/health.js';
import type { Order } from '../src/orders/repository.js';
import { count, openTempDatabase, silentLogger, type TempDatabase, testConfig } from './helpers.js';

const orders = Array.from({ length: 10 }, (_, i) => ({
  customerId: `cust-${String(i % 3)}`,
  lines: [
    { sku: 'SKU-A', qty: i + 1, unitPriceCents: 100 },
    { sku: i % 2 === 0 ? 'SKU-B' : 'SKU-C', qty: 2, unitPriceCents: 250 },
  ],
}));

const expectedProjection = [
  { sku: 'SKU-A', reserved_qty: orders.reduce((sum, o) => sum + (o.lines[0]?.qty ?? 0), 0) },
  { sku: 'SKU-B', reserved_qty: 10 },
  { sku: 'SKU-C', reserved_qty: 10 },
];

async function placeOrders(baseUrl: string): Promise<Order[]> {
  const placed: Order[] = [];
  for (const order of orders) {
    const response = await fetch(`${baseUrl}/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(order),
    });
    expect(response.status).toBe(201);
    placed.push((await response.json()) as Order);
  }
  return placed;
}

const settled = (app: App, tmp: TempDatabase) =>
  vi.waitUntil(() => app.consumer.snapshot().processed === orders.length && count(tmp.db, 'outbox') === count(tmp.db, 'processed_events'), {
    timeout: 10_000,
    interval: 20,
  });

describe('end to end', () => {
  let tmp: TempDatabase;
  let app: App | undefined;
  beforeEach(() => {
    tmp = openTempDatabase();
  });
  afterEach(async () => {
    await app?.stop();
    tmp.cleanup();
  });

  it('delivers every order exactly once to the projection despite rejected and lost publishes', async () => {
    let firstPublishes = 0;
    const broker = new InMemoryBroker({
      injectPublishFault: (_message, attempt) => {
        if (attempt !== 1) return undefined;
        firstPublishes += 1;
        return firstPublishes % 3 === 0 ? 'lost-ack' : firstPublishes % 4 === 0 ? 'reject' : undefined;
      },
    });
    app = createApp({ config: testConfig(), db: tmp.db, broker, logger: silentLogger });
    const port = await app.start(0);
    const baseUrl = `http://127.0.0.1:${String(port)}`;

    const placed = await placeOrders(baseUrl);
    await settled(app, tmp);

    expect(app.relay.snapshot()).toMatchObject({ published: 10, deadLettered: 0 });
    expect(app.relay.snapshot().failed).toBeGreaterThan(0);
    expect(broker.published(ORDER_PLACED).length).toBeGreaterThan(10); // lost acks were republished
    expect(app.consumer.snapshot()).toMatchObject({ processed: 10, deadLettered: 0 });
    expect(app.consumer.snapshot().duplicates).toBeGreaterThan(0);
    expect(readInventoryProjection(tmp.db)).toEqual(expectedProjection);
    expect(count(tmp.db, 'processed_events')).toBe(10);

    const health = (await (await fetch(`${baseUrl}/healthz`)).json()) as Health;
    expect(health).toEqual({ status: 'ok', outbox: { backlog: 0, oldestUnpublishedAgeMs: null }, deadLetters: 0 });
    const fetched = (await (await fetch(`${baseUrl}/orders/${placed[0]?.id ?? ''}`)).json()) as Order;
    expect(fetched).toEqual(placed[0]);
  });

  it('works unchanged over the durable SQLite queue broker', async () => {
    const broker = new SqliteQueueBroker(tmp.db, { pollIntervalMs: 10 });
    app = createApp({ config: testConfig(), db: tmp.db, broker, logger: silentLogger });
    const port = await app.start(0);

    await placeOrders(`http://127.0.0.1:${String(port)}`);
    await settled(app, tmp);

    expect(readInventoryProjection(tmp.db)).toEqual(expectedProjection);
    expect(count(tmp.db, 'queue')).toBe(0);
    expect(app.relay.snapshot()).toEqual({ published: 10, failed: 0, deadLettered: 0 });
  });
});
