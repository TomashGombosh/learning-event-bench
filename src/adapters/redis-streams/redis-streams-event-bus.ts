import Redis from 'ioredis';
import { CollabEvent, EventId, StoredEvent } from '@domain/collab-event';
import { EventBusPort, EventHandler, Subscription } from '@application/ports/event-bus.port';
import { ReplayableEventLogPort } from '@application/ports/replayable-event-log.port';
import { decodeEvent, encodeEvent, roomStreamKey } from '../codec';

type StreamEntries = Array<[string, Array<[string, string[]]>]> | null;

export interface RedisStreamsOptions {
  /** Approximate MAXLEN of each room stream (the replay window L in the reliability model). */
  readonly maxLen: number;
  /** XREAD BLOCK timeout; bounds how fast new room subscriptions are picked up. */
  readonly blockMs: number;
}

/** Event Sourcing on Redis Streams: the room stream is the single source of truth. */
export class RedisStreamsEventBus implements EventBusPort, ReplayableEventLogPort {
  readonly name = 'redis-streams';
  private readonly writer: Redis;
  private readonly reader: Redis;
  private readonly cursors = new Map<string, string>();
  private readonly handlers = new Map<string, Set<EventHandler>>();
  private running = true;
  private loop: Promise<void> | null = null;

  constructor(
    redisUrl: string,
    private readonly options: RedisStreamsOptions = { maxLen: 10_000, blockMs: 100 },
  ) {
    this.writer = new Redis(redisUrl);
    this.reader = new Redis(redisUrl);
  }

  publish(event: CollabEvent): Promise<EventId> {
    return this.writer.xadd(
      roomStreamKey(event.roomId), 'MAXLEN', '~', String(this.options.maxLen), '*',
      'e', encodeEvent(event),
    ) as Promise<EventId>;
  }

  async subscribe(roomId: string, handler: EventHandler): Promise<Subscription> {
    if (!this.handlers.has(roomId)) {
      this.handlers.set(roomId, new Set());
      this.cursors.set(roomId, await this.latestId(roomId));
    }
    this.handlers.get(roomId)!.add(handler);
    this.loop ??= this.readLoop();
    return { unsubscribe: async () => void this.handlers.get(roomId)?.delete(handler) };
  }

  async readAfter(roomId: string, lastId: EventId | null): Promise<StoredEvent[]> {
    const start = lastId === null ? '-' : `(${lastId}`;
    const entries = await this.writer.xrange(roomStreamKey(roomId), start, '+');
    return entries.map(([id, fields]) => ({ id, event: decodeEvent(fields[1]) }));
  }

  async close(): Promise<void> {
    this.running = false;
    this.reader.disconnect();
    await this.loop?.catch(() => undefined);
    this.writer.disconnect();
  }

  private async latestId(roomId: string): Promise<string> {
    const last = await this.writer.xrevrange(roomStreamKey(roomId), '+', '-', 'COUNT', 1);
    return last.length > 0 ? last[0][0] : '0-0';
  }

  private async readLoop(): Promise<void> {
    while (this.running) {
      const rooms = [...this.cursors.keys()];
      const keys = rooms.map(roomStreamKey);
      const ids = rooms.map((r) => this.cursors.get(r)!);
      const result = (await this.reader
        .xread('BLOCK', this.options.blockMs, 'STREAMS', ...keys, ...ids)
        .catch(() => null)) as StreamEntries;
      for (const [key, entries] of result ?? []) this.dispatch(key, entries);
    }
  }

  private dispatch(key: string, entries: Array<[string, string[]]>): void {
    const roomId = key.slice('room:'.length, -':events'.length);
    for (const [id, fields] of entries) {
      this.cursors.set(roomId, id);
      const stored = { id, event: decodeEvent(fields[1]) };
      this.handlers.get(roomId)?.forEach((handle) => handle(stored));
    }
  }
}
