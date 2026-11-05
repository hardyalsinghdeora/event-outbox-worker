import { createApp } from './app.js';
import { SqliteQueueBroker } from './broker/sqliteQueue.js';
import { loadConfig } from './config.js';
import { openDatabase } from './db/database.js';
import { createLogger } from './logger.js';

const config = loadConfig();
const logger = createLogger(config.logLevel);
const db = openDatabase(config.dbPath);
const broker = new SqliteQueueBroker(db, {
  onError: (error) => {
    logger.error({ err: error }, 'broker error');
  },
});
const app = createApp({ config, db, broker, logger });

await app.start(config.port);

const shutdown = (signal: NodeJS.Signals): void => {
  logger.info({ signal }, 'shutting down after the current batch');
  app.stop().then(
    () => process.exit(0),
    (error: unknown) => {
      logger.error({ err: error }, 'shutdown failed');
      process.exit(1);
    },
  );
};
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
