import { createBroker } from '@adapters/broker-factory';
import { runEventBusContract } from './event-bus.contract';

const brokers = process.env.KAFKA_BROKERS;

if (brokers) {
  jest.setTimeout(60_000);
  runEventBusContract('kafka', async () =>
    createBroker('kafka', { redisUrl: '', kafkaBrokers: brokers.split(','), kafkaPartitions: 3, streamMaxLen: 0 }));
} else {
  it.skip('Kafka contract tests (set KAFKA_BROKERS to enable)', () => undefined);
}
