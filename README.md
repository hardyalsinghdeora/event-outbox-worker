# event-outbox-worker

Transactional outbox, at-least-once relay and idempotent consumer, as a small reference
implementation in TypeScript and Node.js. Everything runs in one process for the demo, behind two
ports (`Database`, `Broker`) that keep the three roles separable.

> This is a portfolio sample: a reference implementation written to show how I approach
> event-driven, order-to-cash style systems. It is not, and has never been, in production use.

## The problem

A service that stores an order and publishes `OrderPlaced` writes to two systems that share no
transaction. Whichever order the writes happen in, a crash between them leaves either an order
nobody heard about or an event for an order that does not exist.

## What it demonstrates

| Property | How |
| --- | --- |
| Transactional outbox | `POST /orders` writes the order, its lines and the event's outbox row in one `BEGIN IMMEDIATE … COMMIT` ([ADR 0001](docs/adr/0001-transactional-outbox.md)) |
| At-least-once + idempotent consumer = effectively once | the relay republishes anything the broker did not ack; the consumer dedupes on the event id inside the same transaction as its effect ([ADR 0002](docs/adr/0002-idempotent-consumer.md)) |
| Backoff and dead letters | exponential backoff with jitter on publish and on processing; after `MAX_ATTEMPTS` the event lands in `dead_letters` and stops blocking ([ADR 0003](docs/adr/0003-dead-letter-policy.md)) |
| Per-aggregate ordering | the relay publishes the oldest unpublished row of each aggregate; a failing aggregate stalls only itself |
| Crash-safe relay | nothing is marked published before the broker's ack returns; a lost ack costs a duplicate, never a loss |
| Health and backlog | `GET /healthz` reports outbox backlog, oldest unpublished age and dead-letter count |
| Graceful shutdown | relay and consumer stop on `SIGINT`/`SIGTERM` after the batch or handler in flight |
| Structured logs | pino, every line carries the event id and aggregate id |

## Architecture

```
                 +----------------------- one transaction ------------------------+
 POST /orders -->|  INSERT orders, order_lines           INSERT outbox (envelope)  |
                 +--------------------------------------------+--------------------+
                                                              | poll: due rows, id order,
                                                              | head row per aggregate
                                                              v
                                                        outbox relay --- publish, wait for ack --->  broker
                                                              |                                       |
                                                              | after MAX_ATTEMPTS         deliver     | (at least once)
                                                              |                                       v
                                                              |   +----------------- one transaction -----------------+
                                                              |   |  consumer: seen in processed_events?  -> ack      |
                                                              |   |            else apply inventory_projection       |
                                                              |   |            and INSERT processed_events           |
                                                              |   +-------------------------+-------------------------+
                                                              v                             | after MAX_ATTEMPTS
                                                         dead_letters  <--------------------+
```

The broker is a port with two adapters: `InMemoryBroker` (used by the demo and most tests, with
fault injection) and `SqliteQueueBroker` (a `queue` table with visibility timeouts that another
process could drain; used by `npm run dev` / `npm start`).

## Quick start

Requires Node 22 (`.nvmrc`). Storage is the built-in `node:sqlite`, so there is nothing else to install.

```sh
npm install
npm run demo
```

The demo starts the API, relay and consumer in one process with an in-memory broker that loses the
ack of every third event's first publish, places 10 orders through `POST /orders`, writes one
deliberately malformed event into the outbox, waits for the pipeline to drain and prints:

```
Pipeline
+----------------------------------------------------+----+
| orders placed via POST /orders                     | 10 |
| outbox rows written (orders + 1 poison event)      | 11 |
| publish attempts failed (injected lost acks)       |  3 |
| events published by the relay                      | 11 |
| messages accepted by the broker (incl. duplicates) | 14 |
| events processed by the consumer                   | 10 |
| duplicate deliveries suppressed                    |  3 |
| handler failures nacked for retry                  |  2 |
| dead-lettered after max attempts                   |  1 |
| outbox backlog (GET /healthz)                      |  0 |
| dead letters (GET /healthz)                        |  1 |
+----------------------------------------------------+----+

Inventory projection
+-------+---------------------------+
| SKU-A | 10 reserved (expected 10) |
| SKU-B | 4 reserved (expected 4)   |
| SKU-C | 11 reserved (expected 11) |
| SKU-D | 3 reserved (expected 3)   |
+-------+---------------------------+

Dead letters
+----------------------------------+------------------------------------------------------------+
| inventory-projection / 98c13da1… | ZodError: lines.0.qty: Too small: expected number to be >0 |
+----------------------------------+------------------------------------------------------------+
```

Reading it: three lost acks made the relay republish three events, so the broker saw 14 messages
for 11 outbox rows; the consumer suppressed the three duplicates and the projection matches the
orders exactly. The poison event (`qty: 0`) failed validation on each of its three attempts and
ended in `dead_letters` without touching the projection. `LOG_LEVEL=info npm run demo` shows the
structured logs behind the table.

### Run it as a service

```sh
npm run dev                    # API + relay + consumer with the SQLite queue broker, pretty logs
curl -s localhost:3000/orders -H 'content-type: application/json' \
  -d '{"customerId":"cust-1","lines":[{"sku":"SKU-A","qty":2,"unitPriceCents":1999}]}'
curl -s localhost:3000/healthz
```

`npm run build && npm start` runs the compiled `dist/`. Configuration is read from the environment
(`.env.example` lists every variable; start with `node --env-file=.env --no-warnings=ExperimentalWarning dist/main.js`
to load a file):

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `3000` | HTTP port |
| `DB_PATH` | `./data/outbox.db` | SQLite file (`:memory:` works) |
| `LOG_LEVEL` | `info` | pino level |
| `RELAY_BATCH_SIZE` | `50` | outbox rows per relay poll |
| `MAX_ATTEMPTS` | `5` | publish / handler attempts before dead-lettering |
| `RELAY_POLL_INTERVAL_MS` | `500` | sleep when the outbox is idle |
| `BACKOFF_BASE_MS`, `BACKOFF_MAX_MS` | `500`, `30000` | retry backoff bounds |

### Docker

```sh
docker build -t event-outbox-worker .
docker run --rm -p 3000:3000 -v outbox-data:/data event-outbox-worker
```

Multi-stage build on `node:22-alpine`, runs as the unprivileged `node` user, keeps the database on a
volume at `/data`, and has a `HEALTHCHECK` against `/healthz`.

## Tables

| Table | Why it exists |
| --- | --- |
| `orders`, `order_lines` | the write model; lines are rows so totals and projections can be recomputed |
| `outbox` | the event, written in the order's transaction; `published_at`, `attempts`, `next_attempt_at`, `last_error` are the relay's bookkeeping; `AUTOINCREMENT` keeps ids monotonic for ordering |
| `queue` | storage for `SqliteQueueBroker`: stands in for SQS/RabbitMQ, `visible_at` is the visibility timeout |
| `processed_events` | the consumer's idempotency ledger, keyed by `(consumer, event_id)` |
| `dead_letters` | events a component gave up on, with the error; `consumer` says who gave up (`outbox-relay` or a consumer name) |
| `inventory_projection` | the read model the consumer maintains: reserved quantity per SKU |
| `schema_migrations` | versions applied by the migrator from `src/migrations/*.sql` |

## Design decisions

- **Outbox over dual write.** The event is data in the same transaction as the order; the broker
  publish becomes a retryable background step. Cost: polling latency, an extra table to purge, and
  at-least-once semantics that push idempotency onto consumers.
- **Idempotency at the consumer, not exactly-once delivery.** No mainstream transport gives
  exactly-once delivery across a crash; exactly-once *effect* is cheap at the consumer when the
  ledger row and the effect commit together. Cost: side effects outside the database still need
  their own idempotency keys.
- **Per-aggregate ordering only.** Global ordering would let one failing row block every
  aggregate; order-to-cash rarely needs cross-aggregate order. Cost: head-of-line blocking within
  an aggregate, and a nacked message can be overtaken by a later one of the same aggregate during
  its backoff (the same contract as SQS standard queues or RabbitMQ requeue).
- **Bounded retries, dead letters as a table.** Transient failures recover; permanent ones stop
  blocking after `MAX_ATTEMPTS` and stay inspectable. Cost: a dead-lettered event is a gap in its
  stream until replayed, and validation failures are retried pointlessly a few times.
- **A synchronous `Database` port.** SQLite is synchronous, so a transaction callback runs to
  completion in one tick and nothing else on the shared connection can interleave with it; the
  port refuses async callbacks. A Postgres adapter makes the methods async and hands the callback a
  dedicated client; call sites gain an `await` and nothing else.
- **SQLite here, Postgres there.** `node:sqlite` ships with Node 22 (still flagged experimental,
  hence `--no-warnings=ExperimentalWarning` in the scripts), which keeps the sample dependency-free
  and the tests fast. With Postgres: `SELECT … FOR UPDATE SKIP LOCKED` lets several relay instances
  share the outbox, `LISTEN/NOTIFY` replaces polling, `timestamptz`/`jsonb` replace the ISO strings
  and JSON text. With SQS: `queue.visible_at` is the visibility timeout, `dead_letters` the redrive
  policy; with RabbitMQ: the ack the relay waits for is a publisher confirm.
- **Tooling.** TypeScript 6 rather than 7 because `typescript-eslint` does not support 7 yet;
  strict type-checked lint rules; tests use a fresh SQLite file per test and real timers.

## Deliberately out of scope

- Multi-node relay (leader election or `SKIP LOCKED`); one relay instance is assumed.
- Schema registry and event versioning; the envelope has a `type` but no version.
- Snapshotting or event sourcing; the outbox is a transport, not the system of record.
- Retention jobs for published outbox rows and old `processed_events`.
- Dead-letter replay tooling, authentication, rate limiting, OpenAPI.

## Project layout

```
src/
  db/            Database port over node:sqlite, migrator
  migrations/    versioned SQL applied in order
  orders/        write model: repository (one transaction) and the HTTP API
  outbox/        relay, backoff, outbox repository
  broker/        Broker port, InMemoryBroker, SqliteQueueBroker
  consumer/      idempotent Consumer and the inventory projection handler
  app.ts         wiring of API, relay and consumer; main.ts the service; demo.ts the demo
tests/           vitest, one temp database per test
docs/adr/        the three decisions above, with consequences
```

`npm run check` runs lint, typecheck, tests and the build; CI does the same on every push and pull request.

## About

Built by [Hardyal Singh Deora](https://hardyal.is-a.dev) ([GitHub](https://github.com/hardyalsinghdeora))
as a portfolio sample. It is a reference implementation, not production code. MIT licensed.
