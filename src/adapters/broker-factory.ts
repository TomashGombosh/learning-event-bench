import { EventBusPort } from '@application/ports/event-bus.port';
import { ReplayableEventLogPort } from '@application/ports/replayable-event-log.port';
import { InMemoryEventBus } from './in-memory/in-memory-event-bus';
import { KafkaEventBus } from './kafka/kafka-event-bus';
import { RedisPubSubEventBus } from './redis-pubsub/redis-pubsub-event-bus';
import { RedisStreamsEventBus } from './redis-streams/redis-streams-event-bus';

export type BrokerKind = 'memory' | 'pubsub' | 'streams' | 'kafka' | 'kafka-tuned' | 'kafka-acks';

export interface BrokerBundle {
  readonly bus: EventBusPort;
  readonly log: ReplayableEventLogPort | null;
}

export interface BrokerEnv {
  readonly redisUrl: string;
  readonly kafkaBrokers: string[];
  readonly kafkaPartitions: number;
  readonly streamMaxLen: number;
}

export interface KafkaProducerSettings {
  readonly idempotent: boolean;
  readonly acks: 1 | -1;
  /** null means kafkajs does not cap in-flight produce requests. */
  readonly maxInFlight: number | null;
}

/** The producer the paper should quote. Null for brokers that are not Kafka. */
export function kafkaProducerSettings(kind: string): KafkaProducerSettings | null {
  switch (kind) {
    case 'kafka':
      return { idempotent: true, acks: -1, maxInFlight: 5 };
    case 'kafka-tuned':
      return { idempotent: false, acks: 1, maxInFlight: null };
    case 'kafka-acks':
      return { idempotent: false, acks: -1, maxInFlight: null };
    default:
      return null;
  }
}

/** Composition helper: the only place that knows about concrete broker classes. */
export async function createBroker(kind: BrokerKind, env: BrokerEnv): Promise<BrokerBundle> {
  switch (kind) {
    case 'memory': {
      const bus = new InMemoryEventBus();
      return { bus, log: bus };
    }
    case 'pubsub':
      return { bus: new RedisPubSubEventBus(env.redisUrl), log: null };
    case 'streams': {
      const bus = new RedisStreamsEventBus(env.redisUrl, { maxLen: env.streamMaxLen, blockMs: 100 });
      return { bus, log: bus };
    }
    case 'kafka':
    case 'kafka-tuned':
    case 'kafka-acks': {
      const producer = kafkaProducerSettings(kind)!;
      const bus = new KafkaEventBus({
        brokers: env.kafkaBrokers,
        topic: 'collab-events',
        partitions: env.kafkaPartitions,
        acks: producer.acks,
        idempotent: producer.idempotent,
        maxInFlight: producer.maxInFlight,
        catchUpBuffer: env.streamMaxLen > 0 ? env.streamMaxLen : 10_000,
      });
      await bus.start();
      return { bus, log: bus };
    }
  }
}
