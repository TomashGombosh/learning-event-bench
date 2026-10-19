import { EventCategory } from '@domain/event-category';
import { StoredEvent } from '@domain/collab-event';
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
  it('serves a recent offset from the ring once the live consumer has caught up', () => {
    const buffer = new RoomReplayBuffer(10);
    for (let offset = 0; offset < 5; offset++) buffer.push(stored('room', offset, offset + 1));
    const missed = buffer.slice('room', 0, 2n, 5n);
    expect(missed?.map((item) => item.event.seq)).toEqual([3, 4, 5]);
    expect(buffer.slice('room', 0, 0n, 5n)?.map((item) => item.event.seq)).toEqual([1, 2, 3, 4, 5]);
  });

  it('returns null when the live consumer is behind or the offset has been evicted', () => {
    const buffer = new RoomReplayBuffer(2);
    for (let offset = 0; offset < 5; offset++) buffer.push(stored('room', offset, offset + 1));
    expect(buffer.slice('room', 0, 0n, 5n)).toBeNull();
    expect(buffer.slice('room', 0, 3n, 5n)?.map((item) => item.event.seq)).toEqual([4, 5]);
    expect(buffer.slice('room', 0, 3n, 9n)).toBeNull();
  });
});
