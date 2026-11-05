import { randomUUID } from 'node:crypto';
import { type Clock, isoNow } from '../clock.js';
import type { Database } from '../db/database.js';
import { type EventEnvelope, ORDER_PLACED, type OrderLine, type OrderPlaced } from '../events.js';
import { appendOutboxEvent } from '../outbox/repository.js';

export type OrderStatus = 'placed';

export interface Order {
  id: string;
  customerId: string;
  totalCents: number;
  status: OrderStatus;
  createdAt: string;
  lines: OrderLine[];
}

export interface PlaceOrderInput {
  customerId: string;
  lines: OrderLine[];
}

export interface OrderRepositoryDeps {
  clock: Clock;
  newId?: () => string;
}

interface OrderRow {
  id: string;
  customer_id: string;
  total_cents: number;
  status: OrderStatus;
  created_at: string;
}

interface OrderLineRow {
  sku: string;
  qty: number;
  unit_price_cents: number;
}

/** The order and its OrderPlaced event are committed together or not at all (ADR 0001). */
export function placeOrder(db: Database, input: PlaceOrderInput, { clock, newId = randomUUID }: OrderRepositoryDeps): Order {
  const now = isoNow(clock);
  const order: Order = {
    id: newId(),
    customerId: input.customerId,
    totalCents: input.lines.reduce((sum, line) => sum + line.qty * line.unitPriceCents, 0),
    status: 'placed',
    createdAt: now,
    lines: input.lines,
  };
  const event: EventEnvelope<OrderPlaced> = {
    id: newId(),
    type: ORDER_PLACED,
    occurredAt: now,
    aggregateId: order.id,
    data: { orderId: order.id, customerId: order.customerId, totalCents: order.totalCents, lines: order.lines },
  };

  db.transaction(() => {
    db.run('INSERT INTO orders (id, customer_id, total_cents, status, created_at) VALUES (?, ?, ?, ?, ?)', [
      order.id,
      order.customerId,
      order.totalCents,
      order.status,
      order.createdAt,
    ]);
    order.lines.forEach((line, index) => {
      db.run('INSERT INTO order_lines (order_id, line_no, sku, qty, unit_price_cents) VALUES (?, ?, ?, ?, ?)', [
        order.id,
        index + 1,
        line.sku,
        line.qty,
        line.unitPriceCents,
      ]);
    });
    appendOutboxEvent(db, { aggregateType: 'order', envelope: event, now });
  });
  return order;
}

export function findOrder(db: Database, id: string): Order | undefined {
  const row = db.get<OrderRow>('SELECT id, customer_id, total_cents, status, created_at FROM orders WHERE id = ?', [id]);
  if (!row) return undefined;
  const lines = db.all<OrderLineRow>('SELECT sku, qty, unit_price_cents FROM order_lines WHERE order_id = ? ORDER BY line_no', [id]);
  return {
    id: row.id,
    customerId: row.customer_id,
    totalCents: row.total_cents,
    status: row.status,
    createdAt: row.created_at,
    lines: lines.map((line) => ({ sku: line.sku, qty: line.qty, unitPriceCents: line.unit_price_cents })),
  };
}
