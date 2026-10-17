import { io, Socket } from 'socket.io-client';
import { CollabEvent, StoredEvent } from '@domain/collab-event';
import { EventCategory } from '@domain/event-category';
import { latencyOf } from '@domain/latency';
import { ReplicaDigest } from '@domain/reliability';
import { CatchUpResult } from '@application/use-cases/catch-up.use-case';
import { preciseNowMs } from './clock';
import { LatencyRecorder } from './latency-recorder';
import { PoissonSchedule } from './poisson-schedule';
import { YjsReplica } from './yjs-replica';

export interface ClientCounters {
  sent: number;
  received: number;
  skippedWhileOffline: number;
  catchUpEvents: number;
  catchUpUnsupported: number;
}

export interface MeasurementWindow {
  readonly fromMs: number;
  readonly toMs: number;
}

const ACK_TIMEOUT_MS = 8_000;
const CONNECT_TIMEOUT_MS = 5_000;
const MAX_RECONNECT_ATTEMPTS = 6;
const MAX_CATCH_UP_PAGES = 64;

/**
 * One simulated student: a Yjs replica attached to a gateway via Socket.IO.
 * Join and op acks are timed. Streams catch-up is paged. A killed gateway used
 * to leave `emitWithAck('join')` pending forever (the ack was dropped once the
 * unbounded replay exceeded the socket buffer), which stuck the whole cell.
 */
export class SimulatedClient {
  private socket: Socket | null = null;
  private lastSeenId: string | null = null;
  private seq = 0;
  private online = false;
  private intentionalDisconnect = false;
  private generating = false;
  private gatewayIndex: number;
  /** While a catch-up is in flight, live events must not advance lastSeenId (gap safety). */
  private catchingUp = false;
  private lastLiveIdDuringCatchUp: string | null = null;
  private inFlightReconnect: Promise<void> | null = null;
  /** Outbox: own operations not yet acknowledged by a gateway; re-sent after reconnect. */
  private readonly unacknowledged = new Map<number, CollabEvent>();
  private window: MeasurementWindow = { fromMs: Number.POSITIVE_INFINITY, toMs: Number.POSITIVE_INFINITY };
  /** Pre-encoded one-character Yjs updates. Sending does not touch a Y.Doc. */
  private pool: string[] = [];
  private poolIndex = 0;
  /** Own payloads, applied only when the digest is taken. */
  private ownPayloads: string[] = [];

  constructor(
    readonly clientId: string,
    readonly roomId: string,
    private readonly gatewayUrls: readonly string[],
    private readonly replica: YjsReplica,
    private readonly schedule: PoissonSchedule,
    private readonly recorder: LatencyRecorder,
    readonly counters: ClientCounters,
  ) {
    this.gatewayIndex = Math.abs(hashCode(clientId)) % gatewayUrls.length;
  }

  /**
   * Anchor the open-loop schedule and the measurement window to the moment
   * generation starts. Starting the schedule before `connect()` backfills every
   * missed slot with a timestamp from the past and turns connect time into latency.
   */
  /** Encode `count` updates before the run so the send loop does not build Yjs state. */
  prepare(count: number): void {
    this.pool = this.replica.prebuild(count);
    this.poolIndex = 0;
  }

  arm(startMs: number, warmupS: number, durationS: number): void {
    this.window = {
      fromMs: startMs + warmupS * 1000,
      toMs: startMs + (warmupS + durationS) * 1000,
    };
    this.schedule.reset(startMs);
  }

  async connect(): Promise<void> {
    const socket = io(this.gatewayUrls[this.gatewayIndex], {
      transports: ['websocket'],
      upgrade: false,
      // false disables compression. The published client type only lists the options object.
      // @ts-expect-error engine.io types reject `false` even though the runtime honours it
      perMessageDeflate: false,
      reconnection: false,
      forceNew: true,
    });
    this.socket = socket;
    socket.on('evt', (stored: StoredEvent) => this.onEvent(stored));
    socket.on('disconnect', () => {
      if (socket === this.socket) this.onDisconnect();
    });
    await this.waitUntilConnected(socket);
    await this.catchUp();
    this.online = true;
    this.unacknowledged.forEach((event) => this.emitWithOutbox(event));
  }

  startGenerating(): void {
    this.generating = true;
  }

  stopGenerating(): void {
    this.generating = false;
  }

  /**
   * Called from the shard timer (~1 ms). Sends every arrival that is already due,
   * capped so one stalled client cannot empty the pool inside a single tick.
   */
  sendDue(nowMs: number): void {
    if (!this.generating) return;
    let burst = 0;
    while (burst < 4 && this.schedule.peek() <= nowMs) {
      const scheduledAtMs = this.schedule.advance();
      if (this.online) this.send(scheduledAtMs);
      else this.counters.skippedWhileOffline++;
      burst++;
    }
  }

  /** Simulates a client-side network drop (fault injector). */
  disconnect(): void {
    this.intentionalDisconnect = true;
    this.online = false;
    this.socket?.disconnect();
  }

  /** First connection. Does not skip the schedule; `arm` starts it after everyone is up. */
  open(): Promise<void> {
    this.intentionalDisconnect = false;
    this.inFlightReconnect ??= this.connectWithRetry().finally(() => {
      this.inFlightReconnect = null;
    });
    return this.inFlightReconnect;
  }

  reconnect(): Promise<void> {
    this.intentionalDisconnect = false;
    this.schedule.skipUntil(preciseNowMs());
    this.inFlightReconnect ??= this.connectWithRetry().finally(() => {
      this.inFlightReconnect = null;
    });
    return this.inFlightReconnect;
  }

  get isOnline(): boolean {
    return this.online;
  }

  digest(): ReplicaDigest {
    for (let i = 0; i < this.ownPayloads.length; i++) this.replica.queueRemote(this.ownPayloads[i]);
    this.ownPayloads = [];
    return { roomId: this.roomId, clientId: this.clientId, digest: this.replica.digest() };
  }

  close(): void {
    this.intentionalDisconnect = true;
    this.socket?.disconnect();
  }

  private send(scheduledAtMs: number): void {
    const sentAtMs = preciseNowMs();
    this.recorder.recordScheduleLag(sentAtMs - scheduledAtMs);
    const payload = this.poolIndex < this.pool.length ? this.pool[this.poolIndex++] : this.replica.editRandomly();
    this.ownPayloads.push(payload);
    const event: CollabEvent = {
      roomId: this.roomId,
      clientId: this.clientId,
      seq: ++this.seq,
      category: EventCategory.CollaborativeEditing,
      payload,
      scheduledAtMs,
      sentAtMs,
    };
    this.unacknowledged.set(event.seq, event);
    this.emitWithOutbox(event);
    this.counters.sent++;
  }

  private emitWithOutbox(event: CollabEvent): void {
    const socket = this.socket;
    if (!socket?.connected) return;
    // No per-op timeout. socket.timeout() arms a timer per message; at L5000 that is
    // tens of thousands of timers on one thread and the generator never catches up.
    // A missing ack stays in the outbox and is resent after reconnect.
    socket.emit('op', event, () => {
      this.unacknowledged.delete(event.seq);
    });
  }

  private onEvent(stored: StoredEvent): void {
    if (this.catchingUp) this.lastLiveIdDuringCatchUp = stored.id;
    else this.lastSeenId = stored.id;
    const { event } = stored;
    if (event.clientId === this.clientId) return;
    this.replica.queueRemote(event.payload);
    this.counters.received++;
    if (event.scheduledAtMs < this.window.fromMs || event.scheduledAtMs >= this.window.toMs) return;
    const now = preciseNowMs();
    const sentAt = event.sentAtMs ?? event.scheduledAtMs;
    const serviceUs = (now - sentAt) * 1000;
    if (this.recorder.shouldCapture()) {
      this.recorder.recordServiceUs(serviceUs, latencyOf(
        event.roomId, event.scheduledAtMs, now, event.gatewayInAtMs, event.sentAtMs,
      ));
    } else {
      this.recorder.recordServiceUs(serviceUs);
    }
  }

  /** @returns whether another page is required. */
  private applyCatchUp(result: CatchUpResult): boolean {
    if (!result.supported) {
      this.counters.catchUpUnsupported++;
      return false;
    }
    for (const stored of result.missed) {
      if (stored.event.clientId !== this.clientId) this.replica.queueRemote(stored.event.payload);
      this.lastSeenId = stored.id;
      this.counters.catchUpEvents++;
    }
    return result.truncated;
  }

  /** Unexpected drop (e.g. gateway killed): fail over to the next gateway. */
  private onDisconnect(): void {
    this.online = false;
    if (this.intentionalDisconnect) return;
    this.gatewayIndex = (this.gatewayIndex + 1) % this.gatewayUrls.length;
    setTimeout(() => void this.reconnect(), 200);
  }

  private async connectWithRetry(attempt = 0): Promise<void> {
    try {
      this.socket?.removeAllListeners();
      this.socket?.disconnect();
      await this.connect();
    } catch (error) {
      this.gatewayIndex = (this.gatewayIndex + 1) % this.gatewayUrls.length;
      if (attempt < MAX_RECONNECT_ATTEMPTS) {
        await new Promise((r) => setTimeout(r, 200));
        await this.connectWithRetry(attempt + 1);
        return;
      }
      console.error(
        `reconnect gave up ${this.clientId} after ${MAX_RECONNECT_ATTEMPTS + 1} attempts:`,
        error instanceof Error ? error.message : error,
      );
    }
  }

  private waitUntilConnected(socket: Socket): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error('connect timeout'));
      }, CONNECT_TIMEOUT_MS);
      const cleanup = (): void => {
        clearTimeout(timer);
        socket.off('connect', onConnect);
        socket.off('connect_error', onError);
      };
      const onConnect = (): void => {
        cleanup();
        resolve();
      };
      const onError = (error: Error): void => {
        cleanup();
        reject(error);
      };
      socket.once('connect', onConnect);
      socket.once('connect_error', onError);
    });
  }

  private ack<T>(event: string, body: unknown): Promise<T> {
    const socket = this.socket;
    if (!socket?.connected) return Promise.reject(new Error(`${event}: socket not connected`));
    return socket.timeout(ACK_TIMEOUT_MS).emitWithAck(event, body) as Promise<T>;
  }

  private async catchUp(): Promise<void> {
    this.catchingUp = true;
    this.lastLiveIdDuringCatchUp = null;
    try {
      for (let page = 0; page < MAX_CATCH_UP_PAGES; page++) {
        const before = this.lastSeenId;
        const result = await this.ack<CatchUpResult>('join', {
          roomId: this.roomId,
          lastSeenId: this.lastSeenId,
        });
        const truncated = this.applyCatchUp(result);
        if (!truncated) {
          if (this.lastLiveIdDuringCatchUp !== null) this.lastSeenId = this.lastLiveIdDuringCatchUp;
          return;
        }
        if (this.lastSeenId === before) throw new Error('catch-up made no progress');
      }
      throw new Error('catch-up exceeded page limit');
    } finally {
      this.catchingUp = false;
    }
  }
}

function hashCode(value: string): number {
  let hash = 0;
  for (let i = 0; i < value.length; i++) hash = (Math.imul(31, hash) + value.charCodeAt(i)) | 0;
  return hash;
}
