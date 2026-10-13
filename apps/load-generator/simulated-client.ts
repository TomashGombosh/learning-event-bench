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

/** One simulated student: a Yjs replica attached to a gateway via Socket.IO. */
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

  constructor(
    readonly clientId: string,
    readonly roomId: string,
    private readonly gatewayUrls: readonly string[],
    private readonly replica: YjsReplica,
    private readonly schedule: PoissonSchedule,
    private readonly recorder: LatencyRecorder,
    private readonly window: MeasurementWindow,
    readonly counters: ClientCounters,
  ) {
    this.gatewayIndex = Math.abs(hashCode(clientId)) % gatewayUrls.length;
  }

  async connect(): Promise<void> {
    this.socket = io(this.gatewayUrls[this.gatewayIndex], {
      transports: ['websocket'],
      reconnection: false,
      forceNew: true,
    });
    this.socket.on('evt', (stored: StoredEvent) => this.onEvent(stored));
    const socket = this.socket;
    socket.on('disconnect', () => {
      if (socket === this.socket) this.onDisconnect();
    });
    await new Promise<void>((resolve, reject) => {
      this.socket!.once('connect', () => resolve());
      this.socket!.once('connect_error', reject);
    });
    this.catchingUp = true;
    this.lastLiveIdDuringCatchUp = null;
    try {
      const result: CatchUpResult = await this.socket.emitWithAck('join', {
        roomId: this.roomId,
        lastSeenId: this.lastSeenId,
      });
      this.applyCatchUp(result);
      if (this.lastLiveIdDuringCatchUp !== null) this.lastSeenId = this.lastLiveIdDuringCatchUp;
    } finally {
      this.catchingUp = false;
    }
    this.online = true;
    this.unacknowledged.forEach((event) => this.emitWithOutbox(event));
  }

  startGenerating(): void {
    this.generating = true;
    this.loop();
  }

  stopGenerating(): void {
    this.generating = false;
  }

  /** Simulates a client-side network drop (fault injector). */
  disconnect(): void {
    this.intentionalDisconnect = true;
    this.online = false;
    this.socket?.disconnect();
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
    return { roomId: this.roomId, clientId: this.clientId, digest: this.replica.digest() };
  }

  close(): void {
    this.intentionalDisconnect = true;
    this.socket?.disconnect();
  }

  private loop(): void {
    if (!this.generating) return;
    const delay = Math.max(0, this.schedule.peek() - preciseNowMs());
    setTimeout(() => {
      if (!this.generating) return;
      const scheduledAtMs = this.schedule.advance();
      if (this.online) this.send(scheduledAtMs);
      else this.counters.skippedWhileOffline++;
      this.loop();
    }, delay);
  }

  private send(scheduledAtMs: number): void {
    const event: CollabEvent = {
      roomId: this.roomId,
      clientId: this.clientId,
      seq: ++this.seq,
      category: EventCategory.CollaborativeEditing,
      payload: this.replica.editRandomly(),
      scheduledAtMs,
    };
    this.unacknowledged.set(event.seq, event);
    this.emitWithOutbox(event);
    this.counters.sent++;
  }

  private emitWithOutbox(event: CollabEvent): void {
    this.socket!.emit('op', event, () => this.unacknowledged.delete(event.seq));
  }

  private onEvent(stored: StoredEvent): void {
    if (this.catchingUp) this.lastLiveIdDuringCatchUp = stored.id;
    else this.lastSeenId = stored.id;
    const { event } = stored;
    if (event.clientId === this.clientId) return;
    this.replica.applyRemote(event.payload);
    this.counters.received++;
    if (event.scheduledAtMs >= this.window.fromMs && event.scheduledAtMs < this.window.toMs) {
      this.recorder.record(latencyOf(event.roomId, event.scheduledAtMs, preciseNowMs(), event.gatewayInAtMs));
    }
  }

  private applyCatchUp(result: CatchUpResult): void {
    if (!result.supported) {
      this.counters.catchUpUnsupported++;
      return;
    }
    for (const stored of result.missed) {
      if (stored.event.clientId !== this.clientId) this.replica.applyRemote(stored.event.payload);
      this.lastSeenId = stored.id;
      this.counters.catchUpEvents++;
    }
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
    } catch {
      this.gatewayIndex = (this.gatewayIndex + 1) % this.gatewayUrls.length;
      if (attempt < 50) {
        await new Promise((r) => setTimeout(r, 200));
        await this.connectWithRetry(attempt + 1);
      }
    }
  }
}

function hashCode(value: string): number {
  let hash = 0;
  for (let i = 0; i < value.length; i++) hash = (Math.imul(31, hash) + value.charCodeAt(i)) | 0;
  return hash;
}
