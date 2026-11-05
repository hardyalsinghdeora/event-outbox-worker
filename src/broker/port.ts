export interface Message {
  /** The event id; consumers use it as their idempotency key. */
  id: string;
  topic: string;
  body: string;
}

export interface Delivery {
  message: Message;
  /** 1 for the first delivery of a message, then one more per redelivery. */
  attempt: number;
}

export type Outcome = { kind: 'ack' } | { kind: 'nack'; retryAfterMs: number };

/** Must settle with an outcome; a thrown error is treated as a nack and reported through the broker's `onError`. */
export type MessageHandler = (delivery: Delivery) => Promise<Outcome>;

export interface Subscription {
  /** Stops deliveries; resolves after the handler call in flight has returned. */
  close(): Promise<void>;
}

/** The messaging port: at-least-once delivery, one handler call at a time per subscription. */
export interface Broker {
  /** Resolves only once the broker has durably accepted the message; the relay marks nothing published before that. */
  publish(message: Message): Promise<void>;
  /** A nack is redelivered after `retryAfterMs` with `attempt + 1`. */
  subscribe(topic: string, handler: MessageHandler): Subscription;
}

export const HANDLER_ERROR_RETRY_MS = 1_000;

export async function runHandler(
  handler: MessageHandler,
  delivery: Delivery,
  onError: (error: unknown) => void,
): Promise<Outcome> {
  try {
    return await handler(delivery);
  } catch (error) {
    onError(error);
    return { kind: 'nack', retryAfterMs: HANDLER_ERROR_RETRY_MS };
  }
}
