import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { type Database, openDatabase } from '../src/db/database.js';
import { loadMigrations, migrate } from '../src/db/migrate.js';
import { backoffDelayMs } from '../src/outbox/backoff.js';

describe('migrate', () => {
  let db: Database;
  beforeEach(() => {
    db = openDatabase(':memory:');
  });
  afterEach(() => {
    db.close();
  });

  it('applies every migration once, in version order, and records it', () => {
    expect(migrate(db)).toEqual([1, 2]);
    expect(migrate(db)).toEqual([]);
    expect(db.all('SELECT version, name FROM schema_migrations ORDER BY version')).toEqual([
      { version: 1, name: 'initial' },
      { version: 2, name: 'broker_queue' },
    ]);
    expect(loadMigrations().map((m) => m.version)).toEqual([1, 2]);
  });

  it('rolls back a failing migration entirely', () => {
    const bad = { version: 7, name: 'bad', sql: 'CREATE TABLE half_done (x); INSERT INTO missing_table VALUES (1);' };
    expect(() => migrate(db, { migrations: [bad] })).toThrow(/missing_table/);
    expect(db.all("SELECT name FROM sqlite_master WHERE name = 'half_done'")).toEqual([]);
    expect(db.all('SELECT version FROM schema_migrations')).toEqual([]);
  });
});

describe('Database.transaction', () => {
  it('rolls back when the callback throws and refuses async callbacks', () => {
    const db = openDatabase(':memory:');
    db.exec('CREATE TABLE t (x INTEGER)');
    expect(() =>
      db.transaction(() => {
        db.run('INSERT INTO t VALUES (1)');
        throw new Error('boom');
      }),
    ).toThrow('boom');
    expect(db.all('SELECT x FROM t')).toEqual([]);
    expect(() => db.transaction(() => Promise.resolve())).toThrow(TypeError);
    expect(db.transaction(() => db.run('INSERT INTO t VALUES (2)').changes)).toBe(1);
    db.close();
  });
});

describe('backoffDelayMs', () => {
  const policy = { baseDelayMs: 100, maxDelayMs: 1_000 };

  it('doubles per attempt, caps at the maximum and jitters within the upper half', () => {
    expect(backoffDelayMs(1, policy, () => 0)).toBe(50);
    expect(backoffDelayMs(1, policy, () => 0.999)).toBe(99);
    expect(backoffDelayMs(2, policy, () => 0)).toBe(100);
    expect(backoffDelayMs(3, policy, () => 0)).toBe(200);
    expect(backoffDelayMs(5, policy, () => 0)).toBe(500);
    expect(backoffDelayMs(50, policy, () => 0)).toBe(500);
    expect(backoffDelayMs(50, policy, () => 1)).toBe(1_000);
  });
});
