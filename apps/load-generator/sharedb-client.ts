import { createHash } from 'node:crypto';
import { WebSocket } from 'ws';
import { latencyOf } from '@domain/latency';
import { ReplicaDigest } from '@domain/reliability';
import { OT_COLLECTION } from '@adapters/sharedb/constants';
import { preciseNowMs } from './clock';
import { ConnectGate, FailoverState, initialFailover, noteFailure, pickGateway } from './gateway-failover';
import { LatencyRecorder } from './latency-recorder';
import { PoissonSchedule } from './poisson-schedule';
import { Random } from './random';
import { ReplicaStateStats } from './resources';
import { browserSocket, ShareSocket, sharedbUrl, waitUntilOpen } from './sharedb-socket';
import { ClientCounters, MeasurementWindow } from './simulated-client';

const ShareDBClient = require('sharedb/lib/client');

const ALPHABET = 'abcdefghijklmnopqrstuvwxyz ';
const CONNECT_TIMEOUT_MS = 5_000;
const INITIAL_CONNECT_DEADLINE_MS = 30_000;
const SESSION_DOWN_COOLDOWN_MS = 30_000;
const HANDSHAKE_COOLDOWN_MS = 1_000;
const RECONNECT_BASE_MS = 200;
const RECONNECT_MAX_MS = 2_000;
const reconnectGate = new ConnectGate(32);

interface OpMeta {
  readonly scheduledAtMs: number;
  readonly sentAtMs: number;
  readonly clientId: string;
}

interface ShareDoc {
  preventCompose: boolean;
  submitSource: boolean;
  type: { uri: string } | null;
  data: { text?: string } | null;
  benchOnRemote: ((meta: OpMeta) => void) | null;
  subscribe(callback: (err?: Error | null) => void): void;
  create(data: { text: string }, callback: (err?: Error | null) => void): void;
  fetch(callback: (err?: Error | null) => void): void;
  submitOp(op: unknown, options: { source: OpMeta }, callback: (err?: Error | null) => void): void;
}

interface ShareConnection {
  bindToSocket(socket: unknown): void;
  get(collection: string, id: string): ShareDoc;
  close(): void;
  socket?: ShareSocket;
  on(event: 'error', handler: (error: unknown) => void): void;
}

let opHookInstalled = false;

/** Remote ops carry timing in `x.source`. ShareDB's doc 'op' event does not. */
function installOpHook(): void {
  if (opHookInstalled) return;
  opHookInstalled = true;
  const original = ShareDBClient.Doc.prototype._handleOp;
  ShareDBClient.Doc.prototype._handleOp = function(this: ShareDoc, err: unknown, message: { x?: { source?: OpMeta } }) {
    const meta = message?.x?.source;
    if (!err && meta && typeof meta.scheduledAtMs === 'number' && this.benchOnRemote) this.benchOnRemote(meta);
    return original.call(this, err, message);
  };
}

/**
 * One simulated student on ShareDB json0 text. The socket is a parallel websocket
 * (`/sharedb`) on the same gateway port. Reconnect calls `bindToSocket` so pending
 * ops keep their src/seq (ShareDB dedups a resend). Catch-up is the server op log:
 * subscribe fetches ops the gateway has projected, not the broker replay API.
 */
export class SharedbClient {
  private ws: WebSocket | null = null;
  private connection: ShareConnection | null = null;
  private doc: ShareDoc | null = null;
  private online = false;
  private live = false;
  private intentionalDisconnect = false;
  private generating = false;
  private sessionReady = false;
  private closed = false;
  private readonly failover: FailoverState;
  private pendingFailoverFrom: string | null = null;
  private lastConnectError: unknown = null;
  private inFlightReconnect: Promise<void> | null = null;
  private window: MeasurementWindow = { fromMs: Number.POSITIVE_INFINITY, toMs: Number.POSITIVE_INFINITY };

  constructor(
    readonly clientId: string,
    readonly roomId: string,
    private readonly gatewayUrls: readonly string[],
    private readonly random: Random,
    private readonly schedule: PoissonSchedule,
    private readonly recorder: LatencyRecorder,
    readonly counters: ClientCounters,
  ) {
    this.failover = initialFailover(clientId, gatewayUrls.length);
    installOpHook();
  }

  /** json0 inserts are built from the live document, so there is nothing to prebuild. */
  prepare(count: number): void {
    void count;
  }

  arm(startMs: number, warmupS: number, durationS: number): void {
    this.window = {
      fromMs: startMs + warmupS * 1000,
      toMs: startMs + (warmupS + durationS) * 1000,
    };
    this.schedule.reset(startMs);
  }

  startGenerating(): void {
    this.generating = true;
  }

  stopGenerating(): void {
    this.generating = false;
  }

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

  disconnect(): void {
    this.intentionalDisconnect = true;
    this.online = false;
    this.live = false;
    this.sessionReady = false;
    // Close and mark ShareDB disconnected before any later bindToSocket.
    // The raw socket 'close' event is async, and bindToSocket only accepts
    // a transition out of 'disconnected'.
    this.dropSocket();
  }

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
    return {
      roomId: this.roomId,
      clientId: this.clientId,
      digest: createHash('sha256').update(this.text()).digest('hex'),
    };
  }

  /** The client holds the string. Op-log metadata is measured on the gateway. */
  stateStats(): ReplicaStateStats {
    const textBytes = Buffer.byteLength(this.text());
    return { textBytes, stateBytes: textBytes, metadataBytes: 0 };
  }

  close(): void {
    this.closed = true;
    this.intentionalDisconnect = true;
    this.online = false;
    this.live = false;
    this.ws?.close();
    this.connection?.close();
  }

  private text(): string {
    const text = this.doc?.data?.text;
    return typeof text === 'string' ? text : '';
  }

  private send(scheduledAtMs: number): void {
    const doc = this.doc;
    if (!doc?.type || typeof doc.data?.text !== 'string') {
      this.counters.skippedWhileOffline++;
      return;
    }
    const sentAtMs = preciseNowMs();
    this.recorder.recordScheduleLag(sentAtMs - scheduledAtMs);
    const position = Math.floor(this.random() * (doc.data.text.length + 1));
    const ch = ALPHABET[Math.floor(this.random() * ALPHABET.length)];
    doc.submitOp([{ p: ['text', position], si: ch }], {
      source: { scheduledAtMs, sentAtMs, clientId: this.clientId },
    }, (error) => {
      if (error) console.error(`ot submit ${this.clientId}`, error);
    });
    this.counters.sent++;
    if (scheduledAtMs >= this.window.fromMs && scheduledAtMs < this.window.toMs) this.counters.sentInWindow++;
  }

  private onRemote(meta: OpMeta): void {
    if (meta.clientId === this.clientId) return;
    if (!this.live) {
      this.counters.catchUpEvents++;
      return;
    }
    this.counters.received++;
    if (meta.scheduledAtMs < this.window.fromMs || meta.scheduledAtMs >= this.window.toMs) return;
    const now = preciseNowMs();
    const serviceUs = (now - meta.sentAtMs) * 1000;
    if (this.recorder.shouldCapture()) {
      this.recorder.recordServiceUs(serviceUs, latencyOf(
        this.roomId, meta.scheduledAtMs, now, undefined, meta.sentAtMs,
      ));
    } else {
      this.recorder.recordServiceUs(serviceUs);
    }
  }

  private async connect(): Promise<void> {
    this.sessionReady = false;
    this.live = false;
    await reconnectGate.acquire();
    let ws: WebSocket;
    try {
      ws = new WebSocket(sharedbUrl(this.gatewayUrls[this.failover.index]));
      this.ws = ws;
      ws.on('close', () => {
        if (ws === this.ws) this.onDisconnect();
      });
      await waitUntilOpen(ws, CONNECT_TIMEOUT_MS);
      const socket = browserSocket(ws);
      if (!this.connection) {
        this.connection = new ShareDBClient.Connection(socket) as ShareConnection;
        this.connection.on('error', (error) => {
          console.error(`ot connection ${this.clientId}`, error);
        });
        this.doc = this.connection.get(OT_COLLECTION, this.roomId);
        this.doc.preventCompose = true;
        this.doc.submitSource = true;
        this.doc.benchOnRemote = (meta) => this.onRemote(meta);
      } else {
        this.connection.bindToSocket(socket);
      }
    } finally {
      reconnectGate.release();
    }
    await this.waitSubscribed();
    this.online = true;
    this.sessionReady = true;
    this.live = true;
  }

  private waitSubscribed(): Promise<void> {
    const doc = this.doc;
    if (!doc) return Promise.reject(new Error('connect timeout'));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('connect timeout')), CONNECT_TIMEOUT_MS);
      const fail = (error: Error): void => {
        clearTimeout(timer);
        reject(error);
      };
      const ready = (): void => {
        // History from this subscribe was applied before the callback. Later ops are live.
        this.live = true;
        clearTimeout(timer);
        resolve();
      };
      doc.subscribe((err) => {
        if (err) return fail(err);
        if (doc.type) return ready();
        doc.create({ text: '' }, (createErr) => {
          if (!createErr) return ready();
          doc.fetch((fetchErr) => (fetchErr ? fail(fetchErr) : ready()));
        });
      });
    });
  }

  private dropSocket(): void {
    const socket = this.ws;
    this.ws = null;
    const share = this.connection?.socket;
    if (socket && socket.readyState !== WebSocket.CLOSED) socket.close();
    if (share && share.readyState !== WebSocket.CLOSED) {
      share.readyState = WebSocket.CLOSED;
      share.onclose?.('disconnected');
    }
  }

  private onDisconnect(): void {
    const droppedLiveSession = this.sessionReady;
    this.online = false;
    this.live = false;
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
        this.dropSocket();
        await this.connect();
        if (this.intentionalDisconnect || this.closed) {
          this.online = false;
          this.sessionReady = false;
          this.live = false;
          this.dropSocket();
          return;
        }
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
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
