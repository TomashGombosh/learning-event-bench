import { EventBusPort, Subscription } from '../ports/event-bus.port';
import { RealtimeTransportPort } from '../ports/realtime-transport.port';

/** Keeps exactly one broker subscription per room that has local clients. */
export class RelayRoomEventsUseCase {
  private readonly subscriptions = new Map<string, Promise<Subscription>>();

  constructor(
    private readonly bus: EventBusPort,
    private readonly transport: RealtimeTransportPort,
  ) {}

  async ensureRelaying(roomId: string): Promise<void> {
    if (!this.subscriptions.has(roomId)) {
      const subscription = this.bus.subscribe(roomId, (stored) =>
        this.transport.emitToRoom(roomId, stored),
      );
      this.subscriptions.set(roomId, subscription);
    }
    await this.subscriptions.get(roomId);
  }

  async stopAll(): Promise<void> {
    const pending = [...this.subscriptions.values()];
    this.subscriptions.clear();
    await Promise.all(pending.map(async (s) => (await s).unsubscribe()));
  }
}
