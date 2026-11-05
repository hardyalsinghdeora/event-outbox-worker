import { type Context, Hono } from 'hono';
import type { ContentfulStatusCode } from 'hono/utils/http-status';
import { z } from 'zod';
import type { Clock } from '../clock.js';
import type { Database } from '../db/database.js';
import { orderLineSchema } from '../events.js';
import type { Logger } from '../logger.js';
import { findOrder, placeOrder } from './repository.js';

const placeOrderBody = z.object({
  customerId: z.string().min(1),
  lines: z.array(orderLineSchema).min(1).max(100),
});

export interface ApiDeps {
  db: Database;
  logger: Logger;
  clock: Clock;
}

export function createApi({ db, logger, clock }: ApiDeps): Hono {
  const app = new Hono();

  app.post('/orders', async (c) => {
    const body = placeOrderBody.safeParse(await c.req.json().catch(() => undefined));
    if (!body.success) {
      return problem(c, 400, 'Invalid order', {
        errors: body.error.issues.map((issue) => ({ path: issue.path.map(String).join('.'), message: issue.message })),
      });
    }
    const order = placeOrder(db, body.data, { clock });
    logger.info({ orderId: order.id, totalCents: order.totalCents }, 'order placed');
    return c.json(order, 201, { Location: `/orders/${order.id}` });
  });

  app.get('/orders/:id', (c) => {
    const order = findOrder(db, c.req.param('id'));
    return order ? c.json(order) : problem(c, 404, 'Order not found');
  });

  app.notFound((c) => problem(c, 404, 'Not found'));
  app.onError((error, c) => {
    logger.error({ err: error }, 'unhandled request error');
    return problem(c, 500, 'Internal error');
  });
  return app;
}

/** RFC 9457 problem details; `extra` carries field errors for 400s. */
function problem(c: Context, status: ContentfulStatusCode, title: string, extra: Record<string, unknown> = {}): Response {
  const body = { type: 'about:blank', title, status, instance: c.req.path, ...extra };
  return c.body(JSON.stringify(body), status, { 'Content-Type': 'application/problem+json' });
}
