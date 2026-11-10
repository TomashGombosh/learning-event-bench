import Redis from 'ioredis';

const PubSub = require('sharedb/lib/pubsub');

type StreamEntries = Array<[string, Array<[string, string[]]>]> | null;

const streamKey = (channel: string): string => `sharedb:${channel}`;

/**
 * ShareDB PubSub on Redis Streams. Delivery is the stream order (XADD / XREAD).
 * A subscriber starts at the current tail: this is a pubsub, not the OT catch-up
 * path. Retention is MAXLEN so a gateway that was subscribed for the whole cell
 * can project every op into its in-memory op log.
 */
export function createRedisStreamsPubSub(redisUrl: string, maxLen: number): any {
  function RedisStreamsPubSub(this: StreamsPubSub): void {
    PubSub.call(this);
    this.writer = new Redis(redisUrl);
    this.reader = new Redis(redisUrl);
    this.cursors = new Map();
    this.maxLen = maxLen;
    this.running = true;
    this.loop = null;
  }
  RedisStreamsPubSub.prototype = Object.create(PubSub.prototype);
  RedisStreamsPubSub.prototype.constructor = RedisStreamsPubSub;

  RedisStreamsPubSub.prototype._subscribe = function(this: StreamsPubSub, channel: string, callback: (err?: Error | null) => void): void {
    this.latestId(channel).then((id) => {
      if (!this.cursors.has(channel)) this.cursors.set(channel, id);
      this.loop ??= this.readLoop();
      callback();
    }).catch((err: Error) => callback(err));
  };

  RedisStreamsPubSub.prototype._unsubscribe = function(this: StreamsPubSub, channel: string, callback: (err?: Error | null) => void): void {
    this.cursors.delete(channel);
    callback();
  };

  RedisStreamsPubSub.prototype._publish = function(this: StreamsPubSub, channels: string[], data: unknown, callback: (err?: Error | null) => void): void {
    const payload = JSON.stringify(data);
    const pipeline = this.writer.pipeline();
    for (const channel of channels) {
      pipeline.xadd(streamKey(channel), 'MAXLEN', '~', String(this.maxLen), '*', 'd', payload);
    }
    pipeline.exec().then(() => callback(), (err: Error) => callback(err));
  };

  RedisStreamsPubSub.prototype.close = function(this: StreamsPubSub, callback?: (err?: Error | null) => void): void {
    this.running = false;
    this.reader.disconnect();
    const done = this.loop?.catch(() => undefined) ?? Promise.resolve();
    void done.finally(() => {
      this.writer.disconnect();
      PubSub.prototype.close.call(this, callback);
    });
  };

  RedisStreamsPubSub.prototype.latestId = async function(this: StreamsPubSub, channel: string): Promise<string> {
    const last = await this.writer.xrevrange(streamKey(channel), '+', '-', 'COUNT', 1);
    return last.length > 0 ? last[0][0] : '0-0';
  };

  RedisStreamsPubSub.prototype.readLoop = async function(this: StreamsPubSub): Promise<void> {
    while (this.running) {
      const channels = [...this.cursors.keys()];
      if (channels.length === 0) {
        await delay(50);
        continue;
      }
      const keys = channels.map(streamKey);
      const ids = channels.map((channel) => this.cursors.get(channel)!);
      const result = await this.reader.xread('BLOCK', 100, 'STREAMS', ...keys, ...ids).catch(() => null) as StreamEntries;
      for (const [key, entries] of result ?? []) this.dispatch(key, entries);
    }
  };

  RedisStreamsPubSub.prototype.dispatch = function(this: StreamsPubSub, key: string, entries: Array<[string, string[]]>): void {
    const channel = key.slice('sharedb:'.length);
    for (const [id, fields] of entries) {
      // Unsubscribed while this XREAD was blocked: do not put the cursor back.
      if (!this.cursors.has(channel)) continue;
      this.cursors.set(channel, id);
      this._emit(channel, JSON.parse(fields[1]));
    }
  };

  return new (RedisStreamsPubSub as unknown as new () => StreamsPubSub)();
}

interface StreamsPubSub {
  writer: Redis;
  reader: Redis;
  cursors: Map<string, string>;
  maxLen: number;
  running: boolean;
  loop: Promise<void> | null;
  _emit(channel: string, data: unknown): void;
  latestId(channel: string): Promise<string>;
  readLoop(): Promise<void>;
  dispatch(key: string, entries: Array<[string, string[]]>): void;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
