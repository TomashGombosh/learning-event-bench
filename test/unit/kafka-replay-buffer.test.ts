import { EventCategory } from '@domain/event-category';
import { StoredEvent } from '@domain/collab-event';
import { kafkaProducerSettings } from '@adapters/broker-factory';
import { RoomReplayBuffer } from '@adapters/kafka/kafka-event-bus';

function stored(roomId: string, offset: number, seq: number): StoredEvent {
  return {
    id: `0-${offset}`,
    event: {
      roomId, clientId: 'a', seq, category: EventCategory.CollaborativeEditing,
      payload: '', scheduledAtMs: seq,
    },
  };
}

describe('Kafka room replay buffer', () => {
  it('serves a recent offset from the ring without waiting for the high watermark', () => {
    const buffer = new RoomReplayBuffer(10);
    for (let offset = 0; offset < 5; offset++) buffer.push(stored('room', offset, offset + 1));
    // The watermark can be far ahead. That must not force a new consumer group.
    const missed = buffer.slice('room', 0, 2n);
    expect(missed?.map((item) => item.event.seq)).toEqual([3, 4, 5]);
    expect(buffer.slice('room', 0, 0n)?.map((item) => item.event.seq)).toEqual([1, 2, 3, 4, 5]);
  });

  it('returns null only when the requested offset has been evicted', () => {
    const buffer = new RoomReplayBuffer(2);
    for (let offset = 0; offset < 5; offset++) buffer.push(stored('room', offset, offset + 1));
    expect(buffer.slice('room', 0, 0n)).toBeNull();
    expect(buffer.slice('room', 0, 3n)?.map((item) => item.event.seq)).toEqual([4, 5]);
  });
});

describe('Kafka producer settings recorded on each cell', () => {
  it('separates idempotence, acks, and in-flight cap', () => {
    expect(kafkaProducerSettings('kafka')).toEqual({ idempotent: true, acks: -1, maxInFlight: 5 });
    expect(kafkaProducerSettings('kafka-tuned')).toEqual({ idempotent: false, acks: 1, maxInFlight: null });
    expect(kafkaProducerSettings('kafka-acks')).toEqual({ idempotent: false, acks: -1, maxInFlight: null });
    expect(kafkaProducerSettings('pubsub')).toBeNull();
    expect(kafkaProducerSettings('streams')).toBeNull();
  });
});
