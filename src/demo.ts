import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { createApp } from './app.js';
import { InMemoryBroker } from './broker/inMemory.js';
import { systemClock } from './clock.js';
import { loadConfig } from './config.js';
import { readInventoryProjection } from './consumer/inventoryProjection.js';
import { openDatabase } from './db/database.js';
import { listDeadLetters } from './deadLetters.js';
import { ORDER_PLACED } from './events.js';
import { readHealth } from './health.js';
import { createLogger } from './logger.js';
import { appendOutboxEvent } from './outbox/repository.js';

const ORDERS = [
  { customerId: 'cust-1', lines: [{ sku: 'SKU-A', qty: 2, unitPriceCents: 1_999 }] },
  { customerId: 'cust-2', lines: [{ sku: 'SKU-B', qty: 1, unitPriceCents: 4_500 }] },
  { customerId: 'cust-3', lines: [{ sku: 'SKU-A', qty: 1, unitPriceCents: 1_999 }, { sku: 'SKU-C', qty: 3, unitPriceCents: 250 }] },
  { customerId: 'cust-1', lines: [{ sku: 'SKU-C', qty: 5, unitPriceCents: 250 }] },
  { customerId: 'cust-4', lines: [{ sku: 'SKU-B', qty: 2, unitPriceCents: 4_500 }] },
  { customerId: 'cust-5', lines: [{ sku: 'SKU-A', qty: 4, unitPriceCents: 1_999 }, { sku: 'SKU-B', qty: 1, unitPriceCents: 4_500 }] },
  { customerId: 'cust-2', lines: [{ sku: 'SKU-D', qty: 1, unitPriceCents: 12_000 }] },
  { customerId: 'cust-6', lines: [{ sku: 'SKU-C', qty: 2, unitPriceCents: 250 }] },
  { customerId: 'cust-3', lines: [{ sku: 'SKU-A', qty: 3, unitPriceCents: 1_999 }] },
  { customerId: 'cust-7', lines: [{ sku: 'SKU-D', qty: 2, unitPriceCents: 12_000 }, { sku: 'SKU-C', qty: 1, unitPriceCents: 250 }] },
];

const config = loadConfig({
  DB_PATH: ':memory:',
  LOG_LEVEL: process.env.LOG_LEVEL ?? 'silent',
  MAX_ATTEMPTS: '3',
  RELAY_POLL_INTERVAL_MS: '20',
  BACKOFF_BASE_MS: '20',
  BACKOFF_MAX_MS: '100',
});
const logger = createLogger(config.logLevel);
const db = openDatabase(config.dbPath);

let firstPublishes = 0;
const broker = new InMemoryBroker({
  // Every third event loses its first ack: the broker stored it, so the relay's retry creates a duplicate
  // delivery that the consumer has to suppress.
  injectPublishFault: (_message, attempt) => (attempt === 1 && ++firstPublishes % 3 === 0 ? 'lost-ack' : undefined),
});
const app = createApp({ config, db, broker, logger });
const port = await app.start(0);
const baseUrl = `http://127.0.0.1:${String(port)}`;

for (const order of ORDERS) {
  const response = await fetch(`${baseUrl}/orders`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(order),
  });
  if (response.status !== 201) throw new Error(`POST /orders failed: ${String(response.status)} ${await response.text()}`);
}

// A malformed event written straight into the outbox, as a buggy producer would: qty 0 fails the
// consumer's validation on every attempt and ends up in dead_letters.
const now = new Date().toISOString();
appendOutboxEvent(db, {
  aggregateType: 'order',
  envelope: {
    id: randomUUID(),
    type: ORDER_PLACED,
    occurredAt: now,
    aggregateId: 'order-with-bad-payload',
    data: { orderId: 'order-with-bad-payload', customerId: 'cust-9', totalCents: 0, lines: [{ sku: 'SKU-A', qty: 0, unitPriceCents: 1_999 }] },
  },
  now,
});

const expectedEvents = ORDERS.length + 1;
const settled = await waitUntil(() => {
  const consumer = app.consumer.snapshot();
  return readHealth(db, systemClock).outbox.backlog === 0 && consumer.processed + consumer.deadLettered >= expectedEvents;
}, 10_000);
if (!settled) throw new Error('demo did not settle within 10s');

const relay = app.relay.snapshot();
const consumer = app.consumer.snapshot();
const health = readHealth(db, systemClock);
const expectedReserved = new Map<string, number>();
for (const line of ORDERS.flatMap((order) => order.lines)) {
  expectedReserved.set(line.sku, (expectedReserved.get(line.sku) ?? 0) + line.qty);
}

printTable('Pipeline', [
  ['orders placed via POST /orders', ORDERS.length],
  ['outbox rows written (orders + 1 poison event)', expectedEvents],
  ['publish attempts failed (injected lost acks)', relay.failed],
  ['events published by the relay', relay.published],
  ['messages accepted by the broker (incl. duplicates)', broker.published(ORDER_PLACED).length],
  ['events processed by the consumer', consumer.processed],
  ['duplicate deliveries suppressed', consumer.duplicates],
  ['handler failures nacked for retry', consumer.failed],
  ['dead-lettered after max attempts', consumer.deadLettered],
  ['outbox backlog (GET /healthz)', health.outbox.backlog],
  ['dead letters (GET /healthz)', health.deadLetters],
]);
printTable(
  'Inventory projection',
  readInventoryProjection(db).map((row) => [row.sku, `${String(row.reserved_qty)} reserved (expected ${String(expectedReserved.get(row.sku))})`]),
);
printTable(
  'Dead letters',
  listDeadLetters(db).map((letter) => [`${letter.consumer} / ${letter.event_id.slice(0, 8)}…`, letter.error.split('\n')[0] ?? letter.error]),
);

await app.stop();

async function waitUntil(predicate: () => boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await sleep(25);
  }
  return predicate();
}

function printTable(title: string, rows: [string, string | number][]): void {
  const width = Math.max(...rows.map(([label]) => label.length));
  const valueWidth = Math.max(...rows.map(([, value]) => String(value).length));
  const line = `+${'-'.repeat(width + 2)}+${'-'.repeat(valueWidth + 2)}+`;
  console.log(`\n${title}\n${line}`);
  for (const [label, value] of rows) {
    const padded = typeof value === 'number' ? String(value).padStart(valueWidth) : value.padEnd(valueWidth);
    console.log(`| ${label.padEnd(width)} | ${padded} |`);
  }
  console.log(line);
}
