import { io, Socket } from 'socket.io-client';
import { CollabEvent, StoredEvent } from '@domain/collab-event';
import { EventCategory } from '@domain/event-category';
import { latencyOf } from '@domain/latency';
import { ReplicaDigest } from '@domain/reliability';
import { CatchUpResult } from '@application/use-cases/catch-up.use-case';
import { preciseNowMs } from './clock';
import { ConnectGate, FailoverState, initialFailover, noteFailure, pickGateway } from './gateway-failover';
import { FanoutTracker } from './fanout-tracker';
import { LatencyRecorder } from './latency-recorder';
import { PoissonSchedule } from './poisson-schedule';
import { YjsReplica } from './yjs-replica';

export interface ClientCounters {
  sent: number;
  received: number;
  /** Sends whose scheduled time falls inside the measurement window (excludes warmup). */
  sentInWindow: number;
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
const MAX_CATCH_UP_PAGES = 64;
/** First connection only. Mid-run failover keeps trying until the cell closes. */
const INITIAL_CONNECT_DEADLINE_MS = 30_000;
/** `docker compose kill` does not restart the gateway (`restart: no`). Leave that URL alone. */
const SESSION_DOWN_COOLDOWN_MS = 30_000;
/** A refused handshake (reconnect storm, or the dead port) is retried on the next URL. */
const HANDSHAKE_COOLDOWN_MS = 1_000;
const RECONNECT_BASE_MS = 200;
const RECONNECT_MAX_MS = 2_000;
/** One shard must not open hundreds of websockets in the same millisecond after a kill. */
const reconnectGate = new ConnectGate(32);

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
  private readonly failover: FailoverState;
  /** Set once join/catch-up has succeeded on the current socket. */
  private sessionReady = false;
  private closed = false;
  private pendingFailoverFrom: string | null = null;
  private lastConnectError: unknown = null;
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
    private readonly fanout: FanoutTracker | null = null,
    /** Index among the room's clients. The bitmask in FanoutTracker has 31 bits. */
    private readonly recipientIndex = 0,
    /** Other clients in the room. A window event delivered to all of them is uncensored. */
    private readonly expectedRecipients = 0,
  ) {
    this.failover = initialFailover(clientId, gatewayUrls.length);
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

  private async connect(): Promise<void> {
    this.sessionReady = false;
    // The gate covers the handshake only. Kafka catch-up opens a consumer group
    // (several seconds); holding a slot for that made 333 clients miss the 45 s
    // restore deadline and stay offline.
    await reconnectGate.acquire();
    try {
      const socket = io(this.gatewayUrls[this.failover.index], {
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
    } finally {
      reconnectGate.release();
    }
    await this.catchUp();
    this.online = true;
    this.sessionReady = true;
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
      else {
        this.counters.skippedWhileOffline++;
        if (this.inWindow(scheduledAtMs)) {
          this.fanout?.noteScheduled(this.skipKey(scheduledAtMs), this.expectedRecipients);
        }
      }
      burst++;
    }
  }

  /** Simulates a client-side network drop (fault injector). */
  disconnect(): void {
    this.intentionalDisconnect = true;
    this.online = false;
    this.sessionReady = false;
    this.socket?.disconnect();
  }

  /** First connection. Does not skip the schedule; `arm` starts it after everyone is up. */
  open(): Promise<void> {
    this.intentionalDisconnect = false;
    this.closed = false;
    return this.kickReconnect(INITIAL_CONNECT_DEADLINE_MS);
  }

  reconnect(): Promise<void> {
    this.intentionalDisconnect = false;
    this.closed = false;
    this.schedule.skipUntil(preciseNowMs());
    if (this.online) return Promise.resolve();
    return this.kickReconnect(null);
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
    this.closed = true;
    this.intentionalDisconnect = true;
    this.online = false;
    this.socket?.removeAllListeners();
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
    if (this.inWindow(scheduledAtMs)) {
      this.counters.sentInWindow++;
      this.fanout?.noteScheduled(this.eventKey(this.clientId, event.seq), this.expectedRecipients);
    }
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

  private inWindow(scheduledAtMs: number): boolean {
    return scheduledAtMs >= this.window.fromMs && scheduledAtMs < this.window.toMs;
  }

  private eventKey(senderId: string, seq: number): string {
    return `${this.roomId}\t${senderId}\t${seq}`;
  }

  private skipKey(scheduledAtMs: number): string {
    return `${this.roomId}\t${this.clientId}\tskip\t${scheduledAtMs}`;
  }

  private onEvent(stored: StoredEvent): void {
    if (this.catchingUp) this.lastLiveIdDuringCatchUp = stored.id;
    else this.lastSeenId = stored.id;
    const { event } = stored;
    if (event.clientId === this.clientId) return;
    this.replica.queueRemote(event.payload);
    this.counters.received++;
    if (!this.inWindow(event.scheduledAtMs)) return;
    this.fanout?.noteDelivered(this.eventKey(event.clientId, event.seq), this.recipientIndex);
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

  /**
   * Unexpected drop. The smoke-run clients that logged `reconnect gave up`
   * (c1-9, c40-6, c9-13, c34-6, c2-14, c46-14) all hashed onto gateway2.
   * `docker compose kill` leaves that container down (`restart: no`). Seven
   * attempts at 200 ms still cycled back onto it and then stopped, so those
   * clients never stayed on gateway1 or gateway3. A live session marks that
   * URL down for 30 s. Later handshakes use the other URLs and continue until
   * one accepts or the cell closes.
   */
  private onDisconnect(): void {
    const droppedLiveSession = this.sessionReady;
    this.online = false;
    this.sessionReady = false;
    if (this.intentionalDisconnect || this.closed) return;
    if (droppedLiveSession) {
      const failed = this.failover.index;
      this.pendingFailoverFrom = this.gatewayUrls[failed];
      noteFailure(this.failover, failed, preciseNowMs(), SESSION_DOWN_COOLDOWN_MS);
      this.failover.index = (failed + 1) % this.gatewayUrls.length;
    }
    void this.kickReconnect(null);
  }

  private kickReconnect(deadlineMs: number | null): Promise<void> {
    if (this.closed) return Promise.resolve();
    this.inFlightReconnect ??= this.reconnectLoop(deadlineMs).finally(() => {
      this.inFlightReconnect = null;
    });
    return this.inFlightReconnect;
  }

  private async reconnectLoop(deadlineMs: number | null): Promise<void> {
    const started = preciseNowMs();
    let delayMs = RECONNECT_BASE_MS;
    while (!this.closed && !this.intentionalDisconnect) {
      if (deadlineMs !== null && preciseNowMs() - started >= deadlineMs) {
        const message = this.lastConnectError instanceof Error ? this.lastConnectError.message : this.lastConnectError;
        console.error(`reconnect gave up ${this.clientId} after ${deadlineMs}ms:`, message ?? 'offline');
        return;
      }
      const now = preciseNowMs();
      const index = pickGateway(this.failover, now);
      if (index === null) {
        const soonest = Math.min(...this.failover.blockedUntil);
        await delay(Math.max(50, soonest - now));
        continue;
      }
      try {
        if (this.closed || this.intentionalDisconnect) return;
        this.socket?.removeAllListeners();
        this.socket?.disconnect();
        await this.connect();
        if (this.intentionalDisconnect || this.closed) {
          this.online = false;
          this.sessionReady = false;
          this.socket?.removeAllListeners();
          this.socket?.disconnect();
          return;
        }
        // The socket can drop after join, while this attempt is still the in-flight loop.
        // onDisconnect cannot start a second loop until this one returns, so keep going.
        if (!this.online) continue;
        if (this.pendingFailoverFrom && this.pendingFailoverFrom !== this.gatewayUrls[index]) {
          console.log(`failover ${this.clientId} ${this.pendingFailoverFrom} -> ${this.gatewayUrls[index]}`);
        }
        this.pendingFailoverFrom = null;
        return;
      } catch (error) {
        this.lastConnectError = error;
        noteFailure(this.failover, index, preciseNowMs(), HANDSHAKE_COOLDOWN_MS);
        const jitter = Math.floor(Math.random() * 300);
        await delay(delayMs + jitter);
        delayMs = Math.min(delayMs * 2, RECONNECT_MAX_MS);
      }
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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
