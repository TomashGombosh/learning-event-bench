import { EventId, StoredEvent } from '@domain/collab-event';
import { ReplayableEventLogPort } from '../ports/replayable-event-log.port';

export type CatchUpResult =
  | { readonly supported: true; readonly missed: StoredEvent[] }
  | { readonly supported: false };

/** Returns the events a reconnecting client missed, if the broker can replay them. */
export class CatchUpAfterReconnectUseCase {
  constructor(private readonly log: ReplayableEventLogPort | null) {}

  async execute(roomId: string, lastSeenId: EventId | null): Promise<CatchUpResult> {
    if (this.log === null) return { supported: false };
    return { supported: true, missed: await this.log.readAfter(roomId, lastSeenId) };
  }
}
