import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { DatabaseSync, type SQLInputValue, type StatementSync } from 'node:sqlite';

export type SqlValue = SQLInputValue;

export interface RunResult {
  changes: number;
  lastInsertRowid: number;
}

/**
 * The storage port. Everything above this file depends on this interface, not on node:sqlite.
 *
 * It is synchronous because SQLite is: a transaction callback runs to completion in one tick, so
 * nothing else on this connection can interleave with it. A Postgres adapter (`pg`) would make these
 * methods async and hand the callback a dedicated client; call sites gain an `await`, nothing else.
 */
export interface Database {
  exec(sql: string): void;
  run(sql: string, params?: SqlValue[]): RunResult;
  get<T extends object>(sql: string, params?: SqlValue[]): T | undefined;
  all<T extends object>(sql: string, params?: SqlValue[]): T[];
  /** `BEGIN IMMEDIATE` … `COMMIT`, or `ROLLBACK` when `fn` throws. `fn` must be synchronous. */
  transaction<T>(fn: () => T): T;
  close(): void;
}

export function openDatabase(path: string): Database {
  if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
  // timeout: wait for a concurrent writer (another process draining the queue) instead of failing fast.
  const db = new DatabaseSync(path, { timeout: 5_000 });
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA synchronous = NORMAL');
  db.exec('PRAGMA foreign_keys = ON');
  return new SqliteDatabase(db);
}

class SqliteDatabase implements Database {
  private readonly statements = new Map<string, StatementSync>();

  constructor(private readonly db: DatabaseSync) {}

  exec(sql: string): void {
    this.db.exec(sql);
  }

  run(sql: string, params: SqlValue[] = []): RunResult {
    const result = this.prepare(sql).run(...params);
    return { changes: Number(result.changes), lastInsertRowid: Number(result.lastInsertRowid) };
  }

  get<T extends object>(sql: string, params: SqlValue[] = []): T | undefined {
    return this.prepare(sql).get(...params) as T | undefined;
  }

  all<T extends object>(sql: string, params: SqlValue[] = []): T[] {
    return this.prepare(sql).all(...params) as T[];
  }

  transaction<T>(fn: () => T): T {
    // IMMEDIATE takes the write lock up front, so a transaction never fails with SQLITE_BUSY at COMMIT.
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      if (isThenable(result)) {
        throw new TypeError('transaction callbacks must be synchronous; an async callback would commit before its work');
      }
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  close(): void {
    this.statements.clear();
    this.db.close();
  }

  private prepare(sql: string): StatementSync {
    let statement = this.statements.get(sql);
    if (!statement) {
      statement = this.db.prepare(sql);
      this.statements.set(sql, statement);
    }
    return statement;
  }
}

function isThenable(value: unknown): boolean {
  return typeof value === 'object' && value !== null && 'then' in value && typeof value.then === 'function';
}
