import { EventId, StoredEvent } from '@domain/collab-event';

/**
 * Replay capability, deliberately segregated from EventBusPort (ISP):
 * Redis Pub/Sub cannot implement it, Redis Streams and Kafka can.
 */
export interface ReplayableEventLogPort {
  readAfter(roomId: string, lastId: EventId | null): Promise<StoredEvent[]>;
}

export const EVENT_LOG = Symbol('ReplayableEventLogPort');
