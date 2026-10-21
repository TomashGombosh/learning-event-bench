import type { Consumer } from 'kafkajs';

/**
 * `consumer.run` resolves before the group has joined, and GROUP_JOIN fires
 * before the latest offset is resolved. With `fromBeginning: false` every
 * record published in that window is skipped. Callers wait until the first
 * fetch after the join, which is the point the tail offset is fixed.
 */
export function groupJoined(consumer: Consumer, timeoutMs = 20_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('kafka consumer group join timeout')), timeoutMs);
    let joined = false;
    consumer.on(consumer.events.GROUP_JOIN, () => {
      joined = true;
    });
    consumer.on(consumer.events.FETCH, () => {
      if (!joined) return;
      clearTimeout(timer);
      resolve();
    });
  });
}
