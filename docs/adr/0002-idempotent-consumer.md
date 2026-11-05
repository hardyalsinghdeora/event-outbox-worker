# ADR 0002: Idempotent consumer rather than exactly-once delivery

## Context

The outbox relay and every mainstream broker (SQS standard queues, RabbitMQ, Kafka without
transactions) deliver at least once. Duplicates are not an edge case: a lost ack, a redelivery
after a crash, or an operator replaying a dead letter all produce them. "Exactly-once delivery"
cannot be bought from the transport; exactly-once *effect* can be built at the consumer.

## Decision

Every event carries a UUID in its envelope and the broker message uses it as the message id.
The consumer keeps a `processed_events` ledger keyed by `(consumer, event_id)` and, in one
transaction: checks the ledger, applies the handler's effect, inserts the ledger row. A duplicate
delivery sees the row and acks without touching the projection.

`BEGIN IMMEDIATE` serialises the check and the insert against any other writer, and the primary
key on the ledger is the last line of defence: two concurrent handlers for the same event cannot
both commit.

## Consequences

- The effect and the ledger must live in the same database, which is the case for a projection.
  Side effects outside the database (email, payment capture) need their own idempotency key at
  the receiving service.
- The ledger grows with every event; retention (for example, delete rows older than the broker's
  maximum redelivery window) is an operational task not implemented here.
- Redelivery after a nack lets later events of the same aggregate overtake, exactly as with SQS
  standard queues or RabbitMQ requeue. The projection here is a sum, so order does not matter;
  an order-sensitive consumer would buffer by aggregate version or use a FIFO/partitioned queue.
