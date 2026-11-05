-- Storage for SqliteQueueBroker: a durable queue another process can drain from the same file.
-- visible_at implements SQS-style visibility timeouts for both leases and nack backoff.
CREATE TABLE queue (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  topic       TEXT NOT NULL,
  message_id  TEXT NOT NULL,
  payload     TEXT NOT NULL,
  enqueued_at TEXT NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 0,
  visible_at  TEXT NOT NULL
);

CREATE INDEX queue_visible ON queue (topic, visible_at, id);
