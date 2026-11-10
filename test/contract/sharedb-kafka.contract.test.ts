import { createShareDbPubSub } from '@adapters/sharedb/pubsub-factory';
import { runShareDbPubSubContract } from './pubsub.contract';

const brokers = process.env.KAFKA_BROKERS;

if (brokers) {
  runShareDbPubSubContract('sharedb-kafka-tuned', async () => (await createShareDbPubSub('kafka-tuned', {
    redisUrl: 'redis://localhost:6379',
    kafkaBrokers: brokers.split(','),
    kafkaPartitions: 1,
    streamMaxLen: 1000,
  })).pubsub);
} else {
  it.skip('ShareDB Kafka PubSub contract (set KAFKA_BROKERS to enable)', () => undefined);
}
