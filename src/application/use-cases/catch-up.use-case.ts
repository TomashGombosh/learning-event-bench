import { EventId, StoredEvent } from '@domain/collab-event';
import { ReplayableEventLogPort } from '../ports/replayable-event-log.port';

/**
 * One Socket.IO ack must stay well under the default 1 MB maxHttpBufferSize.
 * A killed gateway reconnects every client in its rooms; an unbounded XRANGE
 * (Redis Streams keeps up to STREAM_MAXLEN) is dropped and the client waits forever.
 */
export const CATCH_UP_PAGE_SIZE = 500;

export type CatchUpResult =
  | {
    readonly supported: true;
    readonly missed: StoredEvent[];
    readonly truncated: boolean;
    /** Id the client should store when `missed` is empty (the log tail, or the cursor it already had). */
    readonly cursor: EventId | null;
  }
  | { readonly supported: false };

/** Returns the events a reconnecting client missed, if the broker can replay them. */
export class CatchUpAfterReconnectUseCase {
  constructor(private readonly log: ReplayableEventLogPort | null) {}

  /**
   * First join (`lastSeenId == null`) returns the tail cursor and no history.
   * Replaying from the start of the log opened a Kafka consumer group per client.
   * The cursor is what a later outage replays from, including an outage that hits
   * before any live event has advanced the client's cursor.
   */
  async open(roomId: string, lastSeenId: EventId | null): Promise<CatchUpResult> {
    if (lastSeenId == null) {
      if (this.log === null) return { supported: true, missed: [], truncated: false, cursor: null };
      const cursor = await this.log.tailId(roomId);
      return { supported: true, missed: [], truncated: false, cursor };
    }
    return this.execute(roomId, lastSeenId);
  }

  async execute(roomId: string, lastSeenId: EventId | null): Promise<CatchUpResult> {
    if (this.log === null) return { supported: false };
    const missed = await this.log.readAfter(roomId, lastSeenId, CATCH_UP_PAGE_SIZE + 1);
    const truncated = missed.length > CATCH_UP_PAGE_SIZE;
    const page = truncated ? missed.slice(0, CATCH_UP_PAGE_SIZE) : missed;
    const cursor = page.length > 0 ? page[page.length - 1].id : lastSeenId;
    return { supported: true, missed: page, truncated, cursor };
  }
}
