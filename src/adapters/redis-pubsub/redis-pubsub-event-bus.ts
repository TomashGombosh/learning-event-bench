import Redis from 'ioredis';
import { CollabEvent, EventId } from '@domain/collab-event';
import { EventBusPort, EventHandler, Subscription } from '@application/ports/event-bus.port';
import { decodeEvent, encodeEvent } from '../codec';

const channelOf = (roomId: string): string => `room:${roomId}`;

/** Fire-and-forget fan-out. No persistence, so it intentionally does NOT implement replay. */
export class RedisPubSubEventBus implements EventBusPort {
  readonly name = 'redis-pubsub';
  private readonly publisher: Redis;
  private readonly subscriber: Redis;
  private readonly handlers = new Map<string, Set<EventHandler>>();
  private sequence = 0;

  constructor(redisUrl: string) {
    this.publisher = new Redis(redisUrl);
    this.subscriber = new Redis(redisUrl);
    this.subscriber.on('message', (channel: string, raw: string) => this.dispatch(channel, raw));
  }

  async publish(event: CollabEvent): Promise<EventId> {
    await this.publisher.publish(channelOf(event.roomId), encodeEvent(event));
    return `${Date.now()}-${++this.sequence}`;
  }

  async subscribe(roomId: string, handler: EventHandler): Promise<Subscription> {
    const channel = channelOf(roomId);
    if (!this.handlers.has(channel)) {
      this.handlers.set(channel, new Set());
      await this.subscriber.subscribe(channel);
    }
    this.handlers.get(channel)!.add(handler);
    return { unsubscribe: async () => this.unsubscribe(channel, handler) };
  }

  async close(): Promise<void> {
    this.handlers.clear();
    this.subscriber.disconnect();
    this.publisher.disconnect();
  }

  private dispatch(channel: string, raw: string): void {
    const event = decodeEvent(raw);
    const stored = { id: `${Date.now()}-${event.clientId}-${event.seq}`, event };
    this.handlers.get(channel)?.forEach((handle) => handle(stored));
  }

  private async unsubscribe(channel: string, handler: EventHandler): Promise<void> {
    const set = this.handlers.get(channel);
    set?.delete(handler);
    if (set && set.size === 0) {
      this.handlers.delete(channel);
      await this.subscriber.unsubscribe(channel);
    }
  }
}
