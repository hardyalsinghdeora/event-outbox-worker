import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { parseEnvelope } from '../src/events.js';
import type { Health } from '../src/health.js';
import { createApi } from '../src/orders/api.js';
import { findOrder, type Order, placeOrder } from '../src/orders/repository.js';
import type { OutboxRow } from '../src/outbox/repository.js';
import { count, failingOn, manualClock, openTempDatabase, silentLogger, type TempDatabase } from './helpers.js';

const input = {
  customerId: 'cust-42',
  lines: [
    { sku: 'SKU-A', qty: 2, unitPriceCents: 1_999 },
    { sku: 'SKU-B', qty: 1, unitPriceCents: 500 },
  ],
};

describe('placeOrder', () => {
  let tmp: TempDatabase;
  beforeEach(() => {
    tmp = openTempDatabase();
  });
  afterEach(() => {
    tmp.cleanup();
  });

  it('commits the order, its lines and the OrderPlaced outbox row together', () => {
    const clock = manualClock();
    const order = placeOrder(tmp.db, input, { clock });

    expect(order).toMatchObject({ customerId: 'cust-42', totalCents: 4_498, status: 'placed', createdAt: '2026-01-01T00:00:00.000Z' });
    expect(findOrder(tmp.db, order.id)).toEqual(order);

    const rows = tmp.db.all<OutboxRow>('SELECT * FROM outbox');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ aggregate_type: 'order', aggregate_id: order.id, event_type: 'OrderPlaced', published_at: null, attempts: 0 });
    expect(parseEnvelope(rows[0]?.payload ?? '')).toMatchObject({
      type: 'OrderPlaced',
      aggregateId: order.id,
      occurredAt: order.createdAt,
      data: { orderId: order.id, customerId: 'cust-42', totalCents: 4_498, lines: input.lines },
    });
  });

  it('persists nothing when the outbox insert fails', () => {
    const faulty = failingOn(tmp.db, 'INSERT INTO outbox');
    expect(() => placeOrder(faulty, input, { clock: manualClock() })).toThrow('injected failure');

    expect(count(tmp.db, 'orders')).toBe(0);
    expect(count(tmp.db, 'order_lines')).toBe(0);
    expect(count(tmp.db, 'outbox')).toBe(0);
    // The connection is usable again: the failed transaction was rolled back, not left open.
    placeOrder(tmp.db, input, { clock: manualClock() });
    expect(count(tmp.db, 'orders')).toBe(1);
    expect(count(tmp.db, 'outbox')).toBe(1);
  });

  it('never has an order without its event', () => {
    for (let i = 0; i < 5; i += 1) placeOrder(tmp.db, input, { clock: manualClock() });
    expect(count(tmp.db, 'orders')).toBe(5);
    expect(count(tmp.db, 'outbox')).toBe(5);
    expect(count(tmp.db, 'order_lines')).toBe(10);
  });
});

describe('orders API', () => {
  let tmp: TempDatabase;
  beforeEach(() => {
    tmp = openTempDatabase();
  });
  afterEach(() => {
    tmp.cleanup();
  });

  const post = (api: ReturnType<typeof createApi>, body: unknown) =>
    api.request('/orders', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });

  it('creates and returns an order', async () => {
    const api = createApi({ db: tmp.db, logger: silentLogger, clock: manualClock() });
    const created = await post(api, input);
    expect(created.status).toBe(201);
    const order = (await created.json()) as Order;
    expect(created.headers.get('location')).toBe(`/orders/${order.id}`);

    const fetched = await api.request(`/orders/${order.id}`);
    expect(fetched.status).toBe(200);
    expect(await fetched.json()).toEqual(order);
  });

  it('answers validation failures and unknown ids with problem+json', async () => {
    const api = createApi({ db: tmp.db, logger: silentLogger, clock: manualClock() });

    const invalid = await post(api, { customerId: 'c', lines: [{ sku: 'SKU-A', qty: 0, unitPriceCents: 1 }] });
    expect(invalid.status).toBe(400);
    expect(invalid.headers.get('content-type')).toBe('application/problem+json');
    expect(await invalid.json()).toMatchObject({ status: 400, title: 'Invalid order', errors: [{ path: 'lines.0.qty' }] });
    expect(count(tmp.db, 'outbox')).toBe(0);

    const malformed = await api.request('/orders', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{not json' });
    expect(malformed.status).toBe(400);

    const missing = await api.request('/orders/does-not-exist');
    expect(missing.status).toBe(404);
    expect(await missing.json()).toMatchObject({ status: 404, title: 'Order not found', instance: '/orders/does-not-exist' });
  });

  it('reports the outbox backlog on /healthz', async () => {
    const clock = manualClock();
    const api = createApi({ db: tmp.db, logger: silentLogger, clock });
    expect((await (await api.request('/healthz')).json()) as Health).toEqual({ status: 'ok', outbox: { backlog: 0, oldestUnpublishedAgeMs: null }, deadLetters: 0 });

    await post(api, input);
    clock.advance(1_500);
    expect((await (await api.request('/healthz')).json()) as Health).toEqual({ status: 'ok', outbox: { backlog: 1, oldestUnpublishedAgeMs: 1_500 }, deadLetters: 0 });
  });
});
