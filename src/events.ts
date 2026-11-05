import { z } from 'zod';

/** Every outbox payload and broker message body is one of these. `id` is the idempotency key end to end. */
export interface EventEnvelope<TData = unknown> {
  id: string;
  type: string;
  occurredAt: string;
  aggregateId: string;
  data: TData;
}

const envelopeSchema = z.object({
  id: z.uuid(),
  type: z.string().min(1),
  occurredAt: z.iso.datetime(),
  aggregateId: z.string().min(1),
  data: z.unknown(),
});

export function parseEnvelope(json: string): EventEnvelope {
  return envelopeSchema.parse(JSON.parse(json));
}

export function tryParseEnvelope(json: string): EventEnvelope | undefined {
  try {
    return parseEnvelope(json);
  } catch {
    return undefined;
  }
}

export const ORDER_PLACED = 'OrderPlaced';

export const orderLineSchema = z.object({
  sku: z.string().min(1),
  qty: z.number().int().positive(),
  unitPriceCents: z.number().int().nonnegative(),
});

export const orderPlacedSchema = z.object({
  orderId: z.string().min(1),
  customerId: z.string().min(1),
  totalCents: z.number().int().nonnegative(),
  lines: z.array(orderLineSchema).min(1),
});

export type OrderLine = z.infer<typeof orderLineSchema>;
export type OrderPlaced = z.infer<typeof orderPlacedSchema>;
