import { createShareDbPubSub } from '@adapters/sharedb/pubsub-factory';
import { runShareDbPubSubContract } from './pubsub.contract';

const redisUrl = process.env.REDIS_URL;
const env = {
  redisUrl: redisUrl ?? 'redis://localhost:6379',
  kafkaBrokers: ['localhost:9092'],
  kafkaPartitions: 1,
  streamMaxLen: 1000,
};

if (redisUrl) {
  runShareDbPubSubContract('sharedb-redis-pubsub', async () => (await createShareDbPubSub('pubsub', env)).pubsub);
  runShareDbPubSubContract('sharedb-redis-streams', async () => (await createShareDbPubSub('streams', env)).pubsub);
} else {
  it.skip('ShareDB Redis PubSub contracts (set REDIS_URL to enable)', () => undefined);
}
