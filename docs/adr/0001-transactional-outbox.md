# ADR 0001: Transactional outbox instead of dual writes

## Context

Placing an order has to do two things: store the order and tell the rest of the system about it.
The database and the message broker are separate systems with no shared transaction. Writing to
both directly ("dual write") fails in both orders: commit then publish can lose the event when the
publish fails, and publish then commit can emit an event for an order that was never stored.

## Decision

The `OrderPlaced` event is written to an `outbox` table in the same local transaction as the order
(`BEGIN IMMEDIATE` … `COMMIT`). A separate relay polls unpublished rows in id order, publishes each
one to the broker, and sets `published_at` only after the broker has acknowledged the message.

The relay publishes only the oldest unpublished row of each aggregate per poll. Rows of one
aggregate therefore reach the broker in id order, and an aggregate whose head row keeps failing
stalls only itself.

## Consequences

- Delivery is at least once. A crash between the broker's ack and the `published_at` update
  republishes the row, so every consumer must be idempotent (ADR 0002).
- Latency is bounded by the poll interval; Postgres `LISTEN/NOTIFY` or a change-data-capture
  reader would remove the polling, at the cost of more moving parts.
- Published rows are kept for inspection; a retention job must purge them eventually.
- One relay instance is assumed. Several instances need `SELECT … FOR UPDATE SKIP LOCKED` (Postgres)
  or a leader election, both out of scope here.
- The per-aggregate head rule publishes at most one event per aggregate per poll; the loop polls
  again immediately while there is work, so this only matters for aggregates with long bursts.
