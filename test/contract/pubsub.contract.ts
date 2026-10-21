/**
 * Liskov contract for ShareDB PubSub adapters.
 * Every adapter (memory, sharedb-redis-pubsub, Redis Streams, Kafka) must honour it.
 * Replay is intentionally not part of the contract: OT catch-up reads the ShareDB op log.
 */

interface PubSubStream {
  on(event: 'data', handler: (data: unknown) => void): void;
  destroy(): void;
}

interface PubSubUnderTest {
  subscribe(channel: string, callback: (err: Error | null, stream: PubSubStream) => void): void;
  publish(channels: string[], data: unknown, callback: (err?: Error | null) => void): void;
  close(callback?: (err?: Error | null) => void): void;
}

let channelCounter = 0;
const freshChannel = (): string => `contract-${Date.now()}-${++channelCounter}`;

function subscribe(pubsub: PubSubUnderTest, channel: string): Promise<unknown[]> {
  return new Promise((resolve, reject) => {
    pubsub.subscribe(channel, (err, stream) => {
      if (err) {
        reject(err);
        return;
      }
      const received: unknown[] = [];
      stream.on('data', (data) => received.push(data));
      resolve(received);
    });
  });
}

function publish(pubsub: PubSubUnderTest, channel: string, data: unknown): Promise<void> {
  return new Promise((resolve, reject) => {
    pubsub.publish([channel], data, (err) => (err ? reject(err) : resolve()));
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timeout waiting for condition');
    await new Promise((r) => setTimeout(r, 10));
  }
}

export function runShareDbPubSubContract(name: string, create: () => Promise<PubSubUnderTest>): void {
  describe(`ShareDB PubSub contract: ${name}`, () => {
    let subject: PubSubUnderTest;

    beforeAll(async () => {
      subject = await create();
    });
    afterAll(async () => {
      await new Promise<void>((resolve, reject) => {
        subject.close((err) => (err ? reject(err) : resolve()));
      });
    });

    it('delivers messages of a channel to its subscriber in publication order', async () => {
      const channel = freshChannel();
      const received = await subscribe(subject, channel);
      for (let i = 1; i <= 50; i++) await publish(subject, channel, { seq: i });
      await waitFor(() => received.length === 50);
      expect(received.map((item) => (item as { seq: number }).seq)).toEqual(
        Array.from({ length: 50 }, (_, i) => i + 1),
      );
    });

    it('isolates channels from each other', async () => {
      const channelA = freshChannel();
      const channelB = freshChannel();
      const receivedA = await subscribe(subject, channelA);
      await publish(subject, channelB, { seq: 1 });
      await publish(subject, channelA, { seq: 2 });
      await waitFor(() => receivedA.length === 1);
      await new Promise((r) => setTimeout(r, 200));
      expect(receivedA).toEqual([{ seq: 2 }]);
    });

    it('preserves the message payload', async () => {
      const channel = freshChannel();
      const received = await subscribe(subject, channel);
      const payload = { op: [{ p: ['text', 0], si: 'a' }], x: { source: { scheduledAtMs: 5, clientId: 'c' } } };
      await publish(subject, channel, payload);
      await waitFor(() => received.length === 1);
      expect(received[0]).toEqual(payload);
    });
  });
}
