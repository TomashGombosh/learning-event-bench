import { RedisPubSubEventBus } from '@adapters/redis-pubsub/redis-pubsub-event-bus';
import { RedisStreamsEventBus } from '@adapters/redis-streams/redis-streams-event-bus';
import { runEventBusContract } from './event-bus.contract';

const redisUrl = process.env.REDIS_URL;

if (redisUrl) {
  runEventBusContract('redis-pubsub', async () => ({ bus: new RedisPubSubEventBus(redisUrl), log: null }));
  runEventBusContract('redis-streams', async () => {
    const bus = new RedisStreamsEventBus(redisUrl, { maxLen: 10_000, blockMs: 50 });
    return { bus, log: bus };
  });
} else {
  it.skip('Redis contract tests (set REDIS_URL to enable)', () => undefined);
}
