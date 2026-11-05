import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { InMemoryBroker } from '../src/broker/inMemory.js';
import { type Delivery, HANDLER_ERROR_RETRY_MS, type Outcome } from '../src/broker/port.js';
import { SqliteQueueBroker } from '../src/broker/sqliteQueue.js';
import { openDatabase } from '../src/db/database.js';
import { count, openTempDatabase, type TempDatabase } from './helpers.js';

const message = (id: string, topic = 'topic') => ({ id, topic, body: `{"id":"${id}"}` });
const ack: Outcome = { kind: 'ack' };
const settle = () => new Promise((resolve) => setTimeout(resolve, 30));

describe('InMemoryBroker', () => {
  it('delivers in publish order, including messages published before anyone subscribed', async () => {
    const broker = new InMemoryBroker();
    await broker.publish(message('1'));
    const seen: Delivery[] = [];
    const subscription = broker.subscribe('topic', (delivery) => {
      seen.push(delivery);
      return Promise.resolve(ack);
    });
    await broker.publish(message('2'));

    await vi.waitUntil(() => seen.length === 2);
    expect(seen.map((d) => [d.message.id, d.attempt])).toEqual([
      ['1', 1],
      ['2', 1],
    ]);
    expect(broker.published('topic').map((m) => m.id)).toEqual(['1', '2']);
    await subscription.close();
  });

  it('redelivers a nacked message after the delay with the attempt incremented', async () => {
    const broker = new InMemoryBroker();
    const attempts: number[] = [];
    const subscription = broker.subscribe('topic', (delivery) => {
      attempts.push(delivery.attempt);
      return Promise.resolve(delivery.attempt < 3 ? { kind: 'nack', retryAfterMs: 5 } : ack);
    });
    await broker.publish(message('1'));

    await vi.waitUntil(() => attempts.length === 3);
    await settle();
    expect(attempts).toEqual([1, 2, 3]);
    await subscription.close();
  });

  it('injects publish faults: reject stores nothing, lost-ack stores the message but fails the call', async () => {
    const broker = new InMemoryBroker({
      injectPublishFault: (m, attempt) => (m.id === 'reject' ? 'reject' : m.id === 'lost' && attempt === 1 ? 'lost-ack' : undefined),
    });
    await expect(broker.publish(message('reject'))).rejects.toThrow('injected publish failure');
    await expect(broker.publish(message('lost'))).rejects.toThrow('injected lost ack');
    await broker.publish(message('lost'));
    expect(broker.published('topic').map((m) => m.id)).toEqual(['lost', 'lost']);
  });

  it('treats a throwing handler as a nack and reports it', async () => {
    const errors: unknown[] = [];
    const broker = new InMemoryBroker({ onError: (error) => errors.push(error) });
    vi.useFakeTimers();
    try {
      let calls = 0;
      const subscription = broker.subscribe('topic', () => {
        calls += 1;
        return calls === 1 ? Promise.reject(new Error('handler bug')) : Promise.resolve(ack);
      });
      await broker.publish(message('1'));
      await vi.advanceTimersByTimeAsync(0);
      expect(errors).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(HANDLER_ERROR_RETRY_MS);
      expect(calls).toBe(2);
      await subscription.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops deliveries on close, waits for the handler in flight and hands pending messages to the next subscriber', async () => {
    const broker = new InMemoryBroker();
    const seen: string[] = [];
    let release: (() => void) | undefined;
    const first = broker.subscribe('topic', (delivery) => {
      seen.push(`first:${delivery.message.id}`);
      return new Promise<Outcome>((resolve) => {
        release = () => {
          resolve(ack);
        };
      });
    });
    await broker.publish(message('1'));
    await broker.publish(message('2'));
    await vi.waitUntil(() => release !== undefined);

    let closed = false;
    const closing = first.close().then(() => {
      closed = true;
    });
    await settle();
    expect(closed).toBe(false); // still waiting for the handler in flight
    release?.();
    await closing;

    expect(() => broker.subscribe('topic', () => Promise.resolve(ack))).not.toThrow();
    await vi.waitUntil(() => broker.published('topic').length === 2 && seen.length === 1);
    expect(seen).toEqual(['first:1']); // message 2 went to the second subscriber, never to the closed one
  });
});

describe('SqliteQueueBroker', () => {
  let tmp: TempDatabase;
  beforeEach(() => {
    tmp = openTempDatabase();
  });
  afterEach(() => {
    tmp.cleanup();
  });

  it('is drained by a consumer on a separate connection, and acks delete the row', async () => {
    const producer = new SqliteQueueBroker(tmp.db);
    await producer.publish(message('1'));
    expect(count(tmp.db, 'queue')).toBe(1);

    const otherConnection = openDatabase(tmp.path);
    const consumer = new SqliteQueueBroker(otherConnection, { pollIntervalMs: 5 });
    const seen: Delivery[] = [];
    const subscription = consumer.subscribe('topic', (delivery) => {
      seen.push(delivery);
      return Promise.resolve(ack);
    });
    await vi.waitUntil(() => seen.length === 1);
    await subscription.close();
    otherConnection.close();

    expect(seen[0]).toEqual({ message: message('1'), attempt: 1 });
    expect(count(tmp.db, 'queue')).toBe(0);
  });

  it('hides a nacked message until its retry time and redelivers it with the next attempt number', async () => {
    const broker = new SqliteQueueBroker(tmp.db, { pollIntervalMs: 5 });
    const attempts: number[] = [];
    const subscription = broker.subscribe('topic', (delivery) => {
      attempts.push(delivery.attempt);
      return Promise.resolve(delivery.attempt === 1 ? { kind: 'nack', retryAfterMs: 300 } : ack);
    });
    await broker.publish(message('1'));

    await vi.waitUntil(() => attempts.length >= 1, { interval: 5 });
    await settle();
    expect(attempts).toEqual([1]); // invisible while the retry delay runs
    await vi.waitUntil(() => attempts.length === 2, { timeout: 2_000, interval: 5 });
    await subscription.close();
    expect(count(tmp.db, 'queue')).toBe(0);
  });

  it('leases a message so a competing consumer cannot take it while it is being handled', async () => {
    const broker = new SqliteQueueBroker(tmp.db, { pollIntervalMs: 5, leaseMs: 10_000 });
    const competitor = new SqliteQueueBroker(openDatabase(tmp.path), { pollIntervalMs: 5, leaseMs: 10_000 });
    let release: (() => void) | undefined;
    const competitorSeen: string[] = [];
    const slow = broker.subscribe('topic', () => new Promise<Outcome>((resolve) => {
      release = () => {
        resolve(ack);
      };
    }));
    const fast = competitor.subscribe('topic', (delivery) => {
      competitorSeen.push(delivery.message.id);
      return Promise.resolve(ack);
    });
    await broker.publish(message('1'));

    await vi.waitUntil(() => release !== undefined);
    await settle();
    expect(competitorSeen).toEqual([]);
    release?.();
    await slow.close();
    await fast.close();
    expect(count(tmp.db, 'queue')).toBe(0);
  });
});
