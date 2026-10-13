import { randomUUID } from 'node:crypto';
import { Admin, Consumer, Kafka, logLevel, Producer } from 'kafkajs';
import { CollabEvent, EventId, StoredEvent } from '@domain/collab-event';
import { EventBusPort, EventHandler, Subscription } from '@application/ports/event-bus.port';
import { ReplayableEventLogPort } from '@application/ports/replayable-event-log.port';
import { decodeEvent, encodeEvent } from '../codec';
import { kafkaEventId, parseKafkaEventId, partitionOf } from './room-partitioner';

export interface KafkaOptions {
  readonly brokers: string[];
  readonly topic: string;
  readonly partitions: number;
  /** acks=1 (latency-tuned) or -1 (all ISR, default durable). */
  readonly acks: 1 | -1;
  readonly idempotent: boolean;
}

/**
 * One topic, rooms mapped to partitions. Every gateway instance uses its own consumer group,
 * so each instance receives all events (broadcast semantics, like the Redis adapters).
 */
export class KafkaEventBus implements EventBusPort, ReplayableEventLogPort {
  readonly name = 'kafka';
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly admin: Admin;
  private readonly handlers = new Map<string, Set<EventHandler>>();
  /** Actual partition count of the topic (may differ from the requested one if it pre-exists). */
  private partitions: number;

  constructor(private readonly options: KafkaOptions) {
    this.kafka = new Kafka({ clientId: 'bench', brokers: options.brokers, logLevel: logLevel.WARN });
    this.producer = this.kafka.producer({ idempotent: options.idempotent, maxInFlightRequests: options.idempotent ? 5 : undefined });
    this.consumer = this.kafka.consumer({ groupId: `gateway-${randomUUID()}`, maxWaitTimeInMs: 5 });
    this.admin = this.kafka.admin();
    this.partitions = options.partitions;
  }

  /** Must be awaited before use (topic creation + group join take seconds). */
  async start(): Promise<void> {
    await this.admin.connect();
    await this.admin.createTopics({
      topics: [{ topic: this.options.topic, numPartitions: this.options.partitions }],
    }).catch(() => false);
    const metadata = await this.admin.fetchTopicMetadata({ topics: [this.options.topic] });
    this.partitions = metadata.topics[0].partitions.length;
    await Promise.all([this.producer.connect(), this.consumer.connect()]);
    await this.consumer.subscribe({ topic: this.options.topic, fromBeginning: false });
    await this.consumer.run({
      eachMessage: async ({ partition, message }) => {
        const event = decodeEvent(message.value!.toString());
        const stored = { id: kafkaEventId(partition, message.offset), event };
        this.handlers.get(event.roomId)?.forEach((handle) => handle(stored));
      },
    });
  }

  async publish(event: CollabEvent): Promise<EventId> {
    const partition = partitionOf(event.roomId, this.partitions);
    const [meta] = await this.producer.send({
      topic: this.options.topic,
      acks: this.options.acks,
      messages: [{ key: event.roomId, partition, value: encodeEvent(event) }],
    });
    return kafkaEventId(partition, meta.baseOffset ?? '0');
  }

  async subscribe(roomId: string, handler: EventHandler): Promise<Subscription> {
    if (!this.handlers.has(roomId)) this.handlers.set(roomId, new Set());
    this.handlers.get(roomId)!.add(handler);
    return { unsubscribe: async () => void this.handlers.get(roomId)?.delete(handler) };
  }

  /** Replays a room partition with a short-lived consumer (slow: includes a group join). */
  async readAfter(roomId: string, lastId: EventId | null): Promise<StoredEvent[]> {
    const partition = partitionOf(roomId, this.partitions);
    const startOffset = lastId === null ? null : parseKafkaEventId(lastId).offset + 1n;
    const offsets = await this.admin.fetchTopicOffsets(this.options.topic);
    const high = BigInt(offsets.find((o) => o.partition === partition)!.high);
    const low = BigInt(offsets.find((o) => o.partition === partition)!.low);
    const from = startOffset ?? low;
    if (from >= high) return [];
    return this.consumeRange(roomId, partition, from, high);
  }

  async close(): Promise<void> {
    await Promise.allSettled([this.consumer.disconnect(), this.producer.disconnect(), this.admin.disconnect()]);
  }

  private async consumeRange(roomId: string, partition: number, from: bigint, high: bigint): Promise<StoredEvent[]> {
    const replayer = this.kafka.consumer({ groupId: `replay-${randomUUID()}`, maxWaitTimeInMs: 5 });
    const collected: StoredEvent[] = [];
    await replayer.connect();
    await replayer.subscribe({ topic: this.options.topic, fromBeginning: true });
    const done = new Promise<void>((resolve) => {
      void replayer.run({
        eachMessage: async ({ partition: p, message }) => {
          if (p !== partition) return;
          const event = decodeEvent(message.value!.toString());
          if (event.roomId === roomId) collected.push({ id: kafkaEventId(p, message.offset), event });
          if (BigInt(message.offset) >= high - 1n) resolve();
        },
      });
    });
    replayer.seek({ topic: this.options.topic, partition, offset: from.toString() });
    await done;
    await replayer.disconnect();
    return collected;
  }
}
