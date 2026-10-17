import { EventId, StoredEvent } from '@domain/collab-event';
import { ReplayableEventLogPort } from '../ports/replayable-event-log.port';

/**
 * One Socket.IO ack must stay well under the default 1 MB maxHttpBufferSize.
 * A killed gateway reconnects every client in its rooms; an unbounded XRANGE
 * (Redis Streams keeps up to STREAM_MAXLEN) is dropped and the client waits forever.
 */
export const CATCH_UP_PAGE_SIZE = 500;

export type CatchUpResult =
  | { readonly supported: true; readonly missed: StoredEvent[]; readonly truncated: boolean }
  | { readonly supported: false };

/** Returns the events a reconnecting client missed, if the broker can replay them. */
export class CatchUpAfterReconnectUseCase {
  constructor(private readonly log: ReplayableEventLogPort | null) {}

  async execute(roomId: string, lastSeenId: EventId | null): Promise<CatchUpResult> {
    if (this.log === null) return { supported: false };
    const missed = await this.log.readAfter(roomId, lastSeenId, CATCH_UP_PAGE_SIZE + 1);
    const truncated = missed.length > CATCH_UP_PAGE_SIZE;
    return { supported: true, missed: truncated ? missed.slice(0, CATCH_UP_PAGE_SIZE) : missed, truncated };
  }
}
