import { EventId, StoredEvent } from '@domain/collab-event';

/**
 * Replay capability, deliberately segregated from EventBusPort (ISP):
 * Redis Pub/Sub cannot implement it, Redis Streams and Kafka can.
 */
export interface ReplayableEventLogPort {
  /**
   * Events strictly after `lastId` (or the whole log when `lastId` is null).
   * `limit` caps the page so a reconnect ack cannot grow without bound.
   */
  readAfter(roomId: string, lastId: EventId | null, limit?: number): Promise<StoredEvent[]>;
}

export const EVENT_LOG = Symbol('ReplayableEventLogPort');
