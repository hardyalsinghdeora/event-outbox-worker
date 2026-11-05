import { type Broker, type Delivery, type Message, type MessageHandler, runHandler, type Subscription } from './port.js';

/** `reject` drops the message; `lost-ack` stores it but fails the call, the way a timed-out network ack does. */
export type PublishFault = 'reject' | 'lost-ack';

export interface InMemoryBrokerOptions {
  /** Consulted on every publish; `attempt` counts publishes of this message id, starting at 1. */
  injectPublishFault?: (message: Message, attempt: number) => PublishFault | undefined;
  onError?: (error: unknown) => void;
}

interface Subscriber {
  handler: MessageHandler;
  closed: boolean;
}

interface Topic {
  log: Message[];
  pending: Delivery[];
  timers: Set<NodeJS.Timeout>;
  subscriber?: Subscriber;
  draining: boolean;
  inFlight: Promise<void>;
}

/**
 * Single-process broker for tests and the demo: one queue per topic, one subscriber per topic,
 * sequential delivery, nack redelivery by timer, and fault injection on publish.
 */
export class InMemoryBroker implements Broker {
  private readonly topics = new Map<string, Topic>();
  private readonly publishAttempts = new Map<string, number>();
  private readonly onError: (error: unknown) => void;

  constructor(private readonly options: InMemoryBrokerOptions = {}) {
    this.onError = options.onError ?? (() => undefined);
  }

  /** Everything ever accepted for a topic, duplicates included. */
  published(topic: string): readonly Message[] {
    return this.topic(topic).log;
  }

  publish(message: Message): Promise<void> {
    const attempt = (this.publishAttempts.get(message.id) ?? 0) + 1;
    this.publishAttempts.set(message.id, attempt);
    const fault = this.options.injectPublishFault?.(message, attempt);
    if (fault === 'reject') return Promise.reject(new Error(`injected publish failure (attempt ${String(attempt)})`));

    const topic = this.topic(message.topic);
    topic.log.push(message);
    topic.pending.push({ message, attempt: 1 });
    this.drain(topic);
    if (fault === 'lost-ack') return Promise.reject(new Error(`injected lost ack (attempt ${String(attempt)})`));
    return Promise.resolve();
  }

  subscribe(topicName: string, handler: MessageHandler): Subscription {
    const topic = this.topic(topicName);
    if (topic.subscriber && !topic.subscriber.closed) throw new Error(`topic ${topicName} already has a subscriber`);
    const subscriber: Subscriber = { handler, closed: false };
    topic.subscriber = subscriber;
    this.drain(topic);
    return {
      close: async () => {
        subscriber.closed = true;
        for (const timer of topic.timers) clearTimeout(timer);
        topic.timers.clear();
        await topic.inFlight;
      },
    };
  }

  private topic(name: string): Topic {
    let topic = this.topics.get(name);
    if (!topic) {
      topic = { log: [], pending: [], timers: new Set(), draining: false, inFlight: Promise.resolve() };
      this.topics.set(name, topic);
    }
    return topic;
  }

  private drain(topic: Topic): void {
    const subscriber = topic.subscriber;
    if (topic.draining || !subscriber || subscriber.closed) return;
    topic.draining = true;
    topic.inFlight = (async () => {
      try {
        while (!subscriber.closed) {
          const delivery = topic.pending.shift();
          if (!delivery) break;
          const outcome = await runHandler(subscriber.handler, delivery, this.onError);
          if (outcome.kind === 'nack') this.scheduleRedelivery(topic, delivery, outcome.retryAfterMs);
        }
      } finally {
        topic.draining = false;
      }
    })();
  }

  private scheduleRedelivery(topic: Topic, delivery: Delivery, delayMs: number): void {
    const redelivery: Delivery = { message: delivery.message, attempt: delivery.attempt + 1 };
    if (topic.subscriber?.closed !== false) {
      // Nobody is listening: keep the message for the next subscriber instead of arming a timer.
      topic.pending.push(redelivery);
      return;
    }
    const timer = setTimeout(() => {
      topic.timers.delete(timer);
      topic.pending.push(redelivery);
      this.drain(topic);
    }, delayMs);
    topic.timers.add(timer);
  }
}
