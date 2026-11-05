# ADR 0003: Bounded retries with a dead-letter table

## Context

Some failures are transient (broker unreachable, lock contention) and go away if retried; some
are permanent (a payload that fails validation, a bug). Retrying forever turns a permanent
failure into a stuck queue; dropping the event turns it into silent data loss.

## Decision

Both the relay (publish failures) and the consumer (handler failures) retry with exponential
backoff plus jitter up to `MAX_ATTEMPTS`. After the last attempt the event is written to
`dead_letters` (upsert on `(consumer, event_id)`, keeping the latest error) and the source is
released: the outbox row is marked published, or the broker message is acked. `GET /healthz`
exposes the dead-letter count so it can be alerted on.

## Consequences

- A dead-lettered event is a gap in its aggregate's stream. Replaying it later restores the data
  only if consumers tolerate reordering; replay tooling is out of scope.
- Permanent failures are retried `MAX_ATTEMPTS` times before giving up. A `NonRetriableError`
  marker that short-circuits to the dead-letter table is the obvious extension.
- Dead letters are rows in the application database, not a broker-level DLQ. That keeps the
  sample self-contained; with SQS the redrive policy would do the same job.
