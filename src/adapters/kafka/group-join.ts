import type { Consumer } from 'kafkajs';

/**
 * `consumer.run` resolves before the group has joined. With `fromBeginning: false`
 * every record published in that window is skipped, so callers wait for GROUP_JOIN.
 */
export function groupJoined(consumer: Consumer, timeoutMs = 20_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('kafka consumer group join timeout')), timeoutMs);
    consumer.on(consumer.events.GROUP_JOIN, () => {
      clearTimeout(timer);
      resolve();
    });
  });
}
