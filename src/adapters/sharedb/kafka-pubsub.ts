import { randomUUID } from 'node:crypto';
import { Consumer, Kafka, logLevel, Producer } from 'kafkajs';
import { kafkaProducerSettings } from '../broker-factory';
import { partitionOf } from '../kafka/room-partitioner';

const PubSub = require('sharedb/lib/pubsub');

export interface KafkaPubSubOptions {
  readonly brokers: string[];
  readonly partitions: number;
  /** `kafka`, `kafka-tuned`, or `kafka-acks`. The CSA matrix uses `kafka-tuned`. */
  readonly kind: string;
  readonly topic?: string;
}

/**
 * ShareDB PubSub on one Kafka topic. Producer settings are the same ones the
 * Yjs path records for that broker kind (`acks`, idempotence, in-flight cap).
 * Each gateway uses its own consumer group so it receives every op (broadcast).
 * Ordering is the partition order of the channel key. Clients do not read Kafka;
 * the gateway projects these messages into the ShareDB op log.
 */
export async function createKafkaPubSub(options: KafkaPubSubOptions): Promise<any> {
  const producerSettings = kafkaProducerSettings(options.kind);
  if (!producerSettings) throw new Error(`not a kafka broker: ${options.kind}`);
  const idempotent = producerSettings.idempotent;
  const acks = producerSettings.acks;
  const maxInFlight = producerSettings.maxInFlight;
  const topic = options.topic ?? 'sharedb-ops';

  function KafkaPubSub(this: KafkaPubSubState): void {
    PubSub.call(this);
    this.topic = topic;
    this.partitions = options.partitions;
    this.acks = acks;
    this.interest = new Set();
    this.kafka = new Kafka({
      clientId: 'bench-sharedb',
      brokers: options.brokers,
      logLevel: logLevel.WARN,
      connectionTimeout: 1_000,
      requestTimeout: 10_000,
    });
    this.producer = this.kafka.producer({
      idempotent,
      maxInFlightRequests: maxInFlight ?? undefined,
    });
    this.consumer = this.kafka.consumer({
      groupId: `sharedb-${randomUUID()}`,
      maxWaitTimeInMs: 5,
      minBytes: 1,
      sessionTimeout: 15_000,
      heartbeatInterval: 1_000,
    });
  }
  KafkaPubSub.prototype = Object.create(PubSub.prototype);
  KafkaPubSub.prototype.constructor = KafkaPubSub;

  KafkaPubSub.prototype._subscribe = function(this: KafkaPubSubState, channel: string, callback: (err?: Error | null) => void): void {
    this.interest.add(channel);
    callback();
  };

  KafkaPubSub.prototype._unsubscribe = function(this: KafkaPubSubState, channel: string, callback: (err?: Error | null) => void): void {
    this.interest.delete(channel);
    callback();
  };

  KafkaPubSub.prototype._publish = function(this: KafkaPubSubState, channels: string[], data: unknown, callback: (err?: Error | null) => void): void {
    const messages = channels.map((channel) => ({
      key: channel,
      partition: partitionOf(channel, this.partitions),
      value: JSON.stringify({ channel, data }),
    }));
    this.producer.send({ topic: this.topic, acks: this.acks, timeout: 5_000, messages })
      .then(() => callback(), (err: Error) => callback(err));
  };

  KafkaPubSub.prototype.close = function(this: KafkaPubSubState, callback?: (err?: Error | null) => void): void {
    void Promise.allSettled([this.consumer.disconnect(), this.producer.disconnect(), this.admin.disconnect()])
      .then(() => PubSub.prototype.close.call(this, callback));
  };

  const pubsub = new (KafkaPubSub as unknown as new () => KafkaPubSubState)();
  const admin = pubsub.kafka.admin();
  pubsub.admin = admin;
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic, numPartitions: options.partitions }],
  }).catch(() => false);
  const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
  pubsub.partitions = metadata.topics[0].partitions.length;
  await Promise.all([pubsub.producer.connect(), pubsub.consumer.connect()]);
  const inFlightLabel = maxInFlight === null ? 'unlimited' : String(maxInFlight);
  console.log(`sharedb producer idempotent=${idempotent} acks=${acks} maxInFlight=${inFlightLabel}`);
  await pubsub.consumer.subscribe({ topic, fromBeginning: false });
  await pubsub.consumer.run({
    eachBatchAutoResolve: true,
    eachBatch: async ({ batch, heartbeat }: { batch: { messages: Array<{ value?: Buffer | null }> }; heartbeat: () => Promise<void> }) => {
      for (let i = 0; i < batch.messages.length; i++) {
        const message = batch.messages[i];
        if (message.value) {
          const parsed = JSON.parse(message.value.toString()) as { channel: string; data: unknown };
          if (pubsub.interest.has(parsed.channel)) pubsub._emit(parsed.channel, parsed.data);
        }
        if (i > 0 && i % 100 === 0) await heartbeat();
      }
    },
  });
  return pubsub;
}

interface KafkaPubSubState {
  topic: string;
  partitions: number;
  acks: 1 | -1;
  interest: Set<string>;
  kafka: Kafka;
  producer: Producer;
  consumer: Consumer;
  admin: ReturnType<Kafka['admin']>;
  _emit(channel: string, data: unknown): void;
}
