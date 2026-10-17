import { CollabEvent, EventId, StoredEvent } from '@domain/collab-event';
import { EventBusPort, EventHandler, Subscription } from '@application/ports/event-bus.port';
import { ReplayableEventLogPort } from '@application/ports/replayable-event-log.port';

/** Reference implementation used by unit/contract tests and as a zero-infrastructure baseline. */
export class InMemoryEventBus implements EventBusPort, ReplayableEventLogPort {
  readonly name = 'in-memory';
  private readonly logs = new Map<string, StoredEvent[]>();
  private readonly handlers = new Map<string, Set<EventHandler>>();

  async publish(event: CollabEvent): Promise<EventId> {
    const log = this.logOf(event.roomId);
    const stored: StoredEvent = { id: String(log.length + 1), event };
    log.push(stored);
    this.handlers.get(event.roomId)?.forEach((handle) => handle(stored));
    return stored.id;
  }

  async subscribe(roomId: string, handler: EventHandler): Promise<Subscription> {
    if (!this.handlers.has(roomId)) this.handlers.set(roomId, new Set());
    this.handlers.get(roomId)!.add(handler);
    return { unsubscribe: async () => void this.handlers.get(roomId)?.delete(handler) };
  }

  async readAfter(roomId: string, lastId: EventId | null, limit?: number): Promise<StoredEvent[]> {
    const from = lastId === null ? 0 : Number(lastId);
    const slice = this.logOf(roomId).slice(from);
    return limit === undefined ? slice : slice.slice(0, limit);
  }

  async close(): Promise<void> {
    this.handlers.clear();
  }

  private logOf(roomId: string): StoredEvent[] {
    if (!this.logs.has(roomId)) this.logs.set(roomId, []);
    return this.logs.get(roomId)!;
  }
}
