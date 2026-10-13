import { EventBusPort } from '@application/ports/event-bus.port';
import { ReplayableEventLogPort } from '@application/ports/replayable-event-log.port';
import { InMemoryEventBus } from './in-memory/in-memory-event-bus';
import { KafkaEventBus } from './kafka/kafka-event-bus';
import { RedisPubSubEventBus } from './redis-pubsub/redis-pubsub-event-bus';
import { RedisStreamsEventBus } from './redis-streams/redis-streams-event-bus';

export type BrokerKind = 'memory' | 'pubsub' | 'streams' | 'kafka' | 'kafka-tuned';

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
    case 'kafka-tuned': {
      const tuned = kind === 'kafka-tuned';
      const bus = new KafkaEventBus({
        brokers: env.kafkaBrokers,
        topic: 'collab-events',
        partitions: env.kafkaPartitions,
        acks: tuned ? 1 : -1,
        idempotent: !tuned,
      });
      await bus.start();
      return { bus, log: bus };
    }
  }
}
