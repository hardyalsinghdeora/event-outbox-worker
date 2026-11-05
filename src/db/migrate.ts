import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { type Clock, isoNow, systemClock } from '../clock.js';
import type { Logger } from '../logger.js';
import type { Database } from './database.js';

// `npm run build` copies src/migrations to dist/migrations, so this resolves in both trees.
const DEFAULT_DIR = fileURLToPath(new URL('../migrations/', import.meta.url));
const FILE_NAME = /^(\d+)_([\w-]+)\.sql$/;

export interface Migration {
  version: number;
  name: string;
  sql: string;
}

export function loadMigrations(dir: string = DEFAULT_DIR): Migration[] {
  const migrations = readdirSync(dir)
    .filter((file) => file.endsWith('.sql'))
    .map((file) => {
      const match = FILE_NAME.exec(file);
      if (!match?.[1] || !match[2]) throw new Error(`Migration file name must look like 0001_name.sql, got ${file}`);
      return { version: Number(match[1]), name: match[2], sql: readFileSync(join(dir, file), 'utf8') };
    })
    .sort((a, b) => a.version - b.version);
  migrations.forEach((m, i) => {
    if (i > 0 && migrations[i - 1]?.version === m.version) throw new Error(`Duplicate migration version ${String(m.version)}`);
  });
  return migrations;
}

export interface MigrateOptions {
  migrations?: Migration[];
  logger?: Logger;
  clock?: Clock;
}

/** Applies pending migrations in version order, each in its own transaction, and records them in schema_migrations. */
export function migrate(db: Database, { migrations = loadMigrations(), logger, clock = systemClock }: MigrateOptions = {}): number[] {
  db.exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TEXT NOT NULL
  )`);
  const applied = new Set(db.all<{ version: number }>('SELECT version FROM schema_migrations').map((row) => row.version));
  const appliedNow: number[] = [];
  for (const migration of migrations) {
    if (applied.has(migration.version)) continue;
    db.transaction(() => {
      db.exec(migration.sql);
      db.run('INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)', [
        migration.version,
        migration.name,
        isoNow(clock),
      ]);
    });
    appliedNow.push(migration.version);
    logger?.info({ version: migration.version, name: migration.name }, 'migration applied');
  }
  return appliedNow;
}
