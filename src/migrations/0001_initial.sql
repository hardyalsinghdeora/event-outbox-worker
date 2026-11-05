-- Write model: the order aggregate. Lines live in their own table so totals can always be recomputed.
CREATE TABLE orders (
  id          TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  total_cents INTEGER NOT NULL CHECK (total_cents >= 0),
  status      TEXT NOT NULL,
  created_at  TEXT NOT NULL
);

CREATE TABLE order_lines (
  order_id         TEXT NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
  line_no          INTEGER NOT NULL,
  sku              TEXT NOT NULL,
  qty              INTEGER NOT NULL CHECK (qty > 0),
  unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
  PRIMARY KEY (order_id, line_no)
) WITHOUT ROWID;

-- Outbox: appended in the same transaction as the aggregate change it describes.
-- AUTOINCREMENT keeps ids monotonic even after published rows are purged; the relay
-- relies on id order for per-aggregate ordering.
CREATE TABLE outbox (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  aggregate_type  TEXT NOT NULL,
  aggregate_id    TEXT NOT NULL,
  event_type      TEXT NOT NULL,
  payload         TEXT NOT NULL CHECK (json_valid(payload)),
  created_at      TEXT NOT NULL,
  published_at    TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,
  last_error      TEXT
);

CREATE INDEX outbox_due ON outbox (next_attempt_at, id) WHERE published_at IS NULL;
CREATE INDEX outbox_aggregate_pending ON outbox (aggregate_type, aggregate_id, id) WHERE published_at IS NULL;

-- Idempotency ledger: one row per (consumer, event), written in the consumer's own transaction.
CREATE TABLE processed_events (
  consumer     TEXT NOT NULL,
  event_id     TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  PRIMARY KEY (consumer, event_id)
) WITHOUT ROWID;

-- Parking lot for events a component gave up on: the relay after max publish attempts,
-- a consumer after max handler attempts. `consumer` names who gave up.
CREATE TABLE dead_letters (
  consumer  TEXT NOT NULL,
  event_id  TEXT NOT NULL,
  payload   TEXT NOT NULL,
  error     TEXT NOT NULL,
  failed_at TEXT NOT NULL,
  PRIMARY KEY (consumer, event_id)
) WITHOUT ROWID;

-- Read model maintained by the inventory consumer.
CREATE TABLE inventory_projection (
  sku          TEXT PRIMARY KEY,
  reserved_qty INTEGER NOT NULL DEFAULT 0 CHECK (reserved_qty >= 0)
) WITHOUT ROWID;
