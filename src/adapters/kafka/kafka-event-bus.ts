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
  /** null: kafkajs does not cap in-flight produce requests. Idempotent producers use 5. */
  readonly maxInFlight: number | null;
  /** Per-room catch-up ring. Recent reconnects read this instead of joining a consumer group. */
  readonly catchUpBuffer?: number;
}

/**
 * Events the live consumer has already applied, capped per room.
 * Catch-up of a recent offset is a slice of this ring. A new consumer group is only
 * opened when the requested offset has already fallen out of the ring, or the live
 * consumer has not yet reached the partition high watermark.
 */
export class RoomReplayBuffer {
  private readonly rooms = new Map<string, StoredEvent[]>();

  constructor(private readonly capacity: number) {}

  push(stored: StoredEvent): void {
    let ring = this.rooms.get(stored.event.roomId);
    if (!ring) {
      ring = [];
      this.rooms.set(stored.event.roomId, ring);
    }
    ring.push(stored);
    if (ring.length > this.capacity) ring.splice(0, ring.length - this.capacity);
  }

  /**
   * Room events on `partition` with offset >= `from`, when the ring still holds `from`.
   * Null means the offset has fallen out of the ring and the caller should read the log.
   * The live consumer being a few records behind the high watermark is not a miss:
   * those records are delivered on the subscription the client just joined.
   */
  slice(roomId: string, partition: number, from: bigint, limit?: number): StoredEvent[] | null {
    const ring = this.rooms.get(roomId);
    if (!ring || ring.length === 0) return null;
    const oldest = parseKafkaEventId(ring[0].id);
    if (oldest.partition !== partition || from < oldest.offset) return null;
    const missed = ring.filter((stored) => {
      const parsed = parseKafkaEventId(stored.id);
      return parsed.partition === partition && parsed.offset >= from;
    });
    return limit === undefined ? missed : missed.slice(0, limit);
  }
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
  private readonly replay: RoomReplayBuffer;

  constructor(private readonly options: KafkaOptions) {
    this.kafka = new Kafka({
      clientId: 'bench',
      brokers: options.brokers,
      logLevel: logLevel.WARN,
      connectionTimeout: 1_000,
      requestTimeout: 10_000,
    });
    // maxInFlightRequests is forwarded to the broker connection. Idempotent producers
    // must stay at or below 5. kafkajs has no linger.ms; each send is one Produce request.
    this.producer = this.kafka.producer({
      idempotent: options.idempotent,
      maxInFlightRequests: options.maxInFlight ?? undefined,
    });
    this.consumer = this.kafka.consumer({
      groupId: `gateway-${randomUUID()}`,
      maxWaitTimeInMs: 5,
      minBytes: 1,
      sessionTimeout: 15_000,
      heartbeatInterval: 1_000,
    });
    this.admin = this.kafka.admin();
    this.partitions = options.partitions;
    this.replay = new RoomReplayBuffer(options.catchUpBuffer ?? 10_000);
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
    const inFlight = this.options.maxInFlight === null ? 'unlimited' : String(this.options.maxInFlight);
    console.log(`producer idempotent=${this.options.idempotent} acks=${this.options.acks} maxInFlight=${inFlight}`);
    await this.consumer.subscribe({ topic: this.options.topic, fromBeginning: false });
    await this.consumer.run({
      eachBatchAutoResolve: true,
      eachBatch: async ({ batch, heartbeat }) => {
        for (let i = 0; i < batch.messages.length; i++) {
          const message = batch.messages[i];
          if (message.value) {
            const event = decodeEvent(message.value.toString());
            const stored = { id: kafkaEventId(batch.partition, message.offset), event };
            this.replay.push(stored);
            this.handlers.get(event.roomId)?.forEach((handle) => handle(stored));
          }
          if (i > 0 && i % 100 === 0) await heartbeat();
        }
        if (batch.messages.length > 0) await heartbeat();
      },
    });
  }

  async publish(event: CollabEvent): Promise<EventId> {
    const partition = partitionOf(event.roomId, this.partitions);
    const [meta] = await this.producer.send({
      topic: this.options.topic,
      acks: this.options.acks,
      timeout: 5_000,
      messages: [{ key: event.roomId, partition, value: encodeEvent(event) }],
    });
    return kafkaEventId(partition, meta.baseOffset ?? '0');
  }

  async subscribe(roomId: string, handler: EventHandler): Promise<Subscription> {
    if (!this.handlers.has(roomId)) this.handlers.set(roomId, new Set());
    this.handlers.get(roomId)!.add(handler);
    return { unsubscribe: async () => void this.handlers.get(roomId)?.delete(handler) };
  }

  /**
   * Recent catch-up is a slice of the live consumer's ring (no extra group join).
   * `consumeRange` runs only when that ring does not cover the requested offset.
   */
  async readAfter(roomId: string, lastId: EventId | null, limit?: number): Promise<StoredEvent[]> {
    const partition = partitionOf(roomId, this.partitions);
    // Do not compare the ring to the high watermark. Under load the watermark moves
    // on every produce, so that check made every reconnect fall through to a new
    // consumer group (cell 011: ν=1, offline fraction 0.27, 47 clients still down).
    if (lastId !== null) {
      const from = parseKafkaEventId(lastId).offset + 1n;
      const buffered = this.replay.slice(roomId, partition, from, limit);
      if (buffered !== null) return buffered;
    }
    const startOffset = lastId === null ? null : parseKafkaEventId(lastId).offset + 1n;
    const offsets = await this.admin.fetchTopicOffsets(this.options.topic);
    const high = BigInt(offsets.find((o) => o.partition === partition)!.high);
    const low = BigInt(offsets.find((o) => o.partition === partition)!.low);
    const from = startOffset ?? low;
    if (from >= high) return [];
    console.warn(`kafka catch-up fallback room=${roomId} from=${from} ring does not cover the offset`);
    return this.consumeRange(roomId, partition, from, high, limit);
  }

  async close(): Promise<void> {
    await Promise.allSettled([this.consumer.disconnect(), this.producer.disconnect(), this.admin.disconnect()]);
  }

  /** Fallback when the ring does not cover `from`. One short-lived group per call. */
  private async consumeRange(
    roomId: string, partition: number, from: bigint, high: bigint, limit?: number,
  ): Promise<StoredEvent[]> {
    const replayer = this.kafka.consumer({ groupId: `replay-${randomUUID()}`, maxWaitTimeInMs: 5 });
    const collected: StoredEvent[] = [];
    await replayer.connect();
    await replayer.subscribe({ topic: this.options.topic, fromBeginning: true });
    const done = new Promise<void>((resolve) => {
      void replayer.run({
        eachMessage: async ({ partition: p, message }) => {
          if (p !== partition) return;
          const event = decodeEvent(message.value!.toString());
          if (event.roomId === roomId) {
            collected.push({ id: kafkaEventId(p, message.offset), event });
            if (limit !== undefined && collected.length >= limit) resolve();
          }
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
