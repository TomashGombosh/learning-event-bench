import { KafkaProducerSettings, kafkaProducerSettings } from '../broker-factory';
import { createKafkaPubSub } from './kafka-pubsub';
import { createRedisStreamsPubSub } from './redis-streams-pubsub';

const ShareDB = require('sharedb');

export interface ShareDbPubSubEnv {
  readonly redisUrl: string;
  readonly kafkaBrokers: string[];
  readonly kafkaPartitions: number;
  readonly streamMaxLen: number;
}

export interface ShareDbPubSubBundle {
  readonly pubsub: any;
  readonly close: () => Promise<void>;
  /** Null when the transport is not Kafka. */
  readonly producer: KafkaProducerSettings | null;
}

/**
 * Inter-gateway OT propagation. `pubsub` is sharedb-redis-pubsub. Streams and
 * Kafka are local PubSub subclasses. `memory` is ShareDB's in-process pubsub
 * (one gateway; it does not cross processes).
 */
export async function createShareDbPubSub(kind: string, env: ShareDbPubSubEnv): Promise<ShareDbPubSubBundle> {
  switch (kind) {
    case 'memory': {
      const pubsub = new ShareDB.MemoryPubSub();
      return { pubsub, producer: null, close: () => closePubSub(pubsub) };
    }
    case 'pubsub': {
      const pubsub = await createRedisShareDbPubSub(env.redisUrl);
      return { pubsub, producer: null, close: () => closePubSub(pubsub) };
    }
    case 'streams': {
      const pubsub = createRedisStreamsPubSub(env.redisUrl, env.streamMaxLen);
      return { pubsub, producer: null, close: () => closePubSub(pubsub) };
    }
    case 'kafka':
    case 'kafka-tuned':
    case 'kafka-acks': {
      const pubsub = await createKafkaPubSub({
        brokers: env.kafkaBrokers,
        partitions: env.kafkaPartitions,
        kind,
      });
      return { pubsub, producer: kafkaProducerSettings(kind), close: () => closePubSub(pubsub) };
    }
    default:
      throw new Error(`unsupported OT broker: ${kind}`);
  }
}

async function createRedisShareDbPubSub(redisUrl: string): Promise<any> {
  const redis = require('redis');
  const RedisPubSub = require('sharedb-redis-pubsub');
  const client = redis.createClient({ url: redisUrl });
  const observer = redis.createClient({ url: redisUrl });
  await Promise.all([client.connect(), observer.connect()]);
  return new RedisPubSub({ client, observer });
}

function closePubSub(pubsub: { close: (callback?: (err?: Error | null) => void) => void }): Promise<void> {
  return new Promise((resolve, reject) => {
    pubsub.close((err) => (err ? reject(err) : resolve()));
  });
}
