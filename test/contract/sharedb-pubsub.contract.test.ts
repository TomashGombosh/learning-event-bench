import { createShareDbPubSub } from '@adapters/sharedb/pubsub-factory';
import { runShareDbPubSubContract } from './pubsub.contract';

runShareDbPubSubContract('memory', async () => {
  const bundle = await createShareDbPubSub('memory', {
    redisUrl: 'redis://localhost:6379',
    kafkaBrokers: ['localhost:9092'],
    kafkaPartitions: 1,
    streamMaxLen: 100,
  });
  return bundle.pubsub;
});
