import { createHash } from 'node:crypto';
import { WebSocket } from 'ws';
import { latencyOf } from '@domain/latency';
import { ReplicaDigest } from '@domain/reliability';
import { OT_COLLECTION } from '@adapters/sharedb/constants';
import { preciseNowMs } from './clock';
import { ConnectGate, FailoverState, initialFailover, noteFailure, pickGateway } from './gateway-failover';
import { FanoutTracker } from './fanout-tracker';
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
const CREATE_FALLBACK_MS = 500;
const reconnectGate = new ConnectGate(32);

interface OpMeta {
  readonly scheduledAtMs: number;
  readonly sentAtMs: number;
  readonly clientId: string;
  readonly seq: number;
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
  on(event: string, listener: (...args: unknown[]) => void): void;
  once(event: string, listener: (...args: unknown[]) => void): void;
  off(event: string, listener: (...args: unknown[]) => void): void;
}

interface ShareConnection {
  bindToSocket(socket: unknown): void;
  get(collection: string, id: string): ShareDoc;
  close(): void;
  socket?: ShareSocket;
  on(event: 'error', handler: (error: unknown) => void): void;
}

let opHookInstalled = false;
const FAST_TRANSFORM_X = Symbol('fastTransformX');

type TaggedTransformX = ((left: unknown, right: unknown) => [unknown, unknown]) & {
  [FAST_TRANSFORM_X]?: boolean;
};

/**
 * Remote ops carry timing in `x.source`. ShareDB's doc 'op' event does not.
 * Also override `ot-json0`'s `bootstrapTransform` `transformX` (which already
 * exists on `defaultType` and deep-clones ops via JSON.parse/stringify) with
 * a fast path for single-key text inserts so backed-up `pendingOps` under
 * saturation do not pin loadgen CPU.
 */
function installOpHook(): void {
  if (opHookInstalled) return;
  opHookInstalled = true;
  const original = ShareDBClient.Doc.prototype._handleOp;
  ShareDBClient.Doc.prototype._handleOp = function(this: ShareDoc, err: unknown, message: { x?: { source?: OpMeta } }) {
    const meta = message?.x?.source;
    if (!err && meta && typeof meta.scheduledAtMs === 'number' && this.benchOnRemote) this.benchOnRemote(meta);
    return original.call(this, err, message);
  };
  const defaultType = ShareDBClient.types?.defaultType;
  if (defaultType && typeof defaultType.transform === 'function' && !(defaultType.transformX as TaggedTransformX | undefined)?.[FAST_TRANSFORM_X]) {
    const origTransform = defaultType.transform.bind(defaultType);
    const fastTransformX: TaggedTransformX = function(left: unknown, right: unknown): [unknown, unknown] {
      if (
        Array.isArray(left) && Array.isArray(right) &&
        left.length === 1 && right.length === 1
      ) {
        const a = left[0] as { p?: unknown[]; si?: unknown };
        const b = right[0] as { p?: unknown[]; si?: unknown };
        if (
          typeof a?.si === 'string' && typeof b?.si === 'string' &&
          a.si.length > 0 && b.si.length > 0 &&
          Array.isArray(a.p) && Array.isArray(b.p) &&
          a.p.length === 2 && b.p.length === 2 &&
          a.p[0] === b.p[0] &&
          typeof a.p[1] === 'number' && typeof b.p[1] === 'number'
        ) {
          const key = a.p[0];
          const pa = a.p[1];
          const pb = b.p[1];
          if (pa <= pb) {
            return [
              [{ p: [key, pa], si: a.si }],
              [{ p: [key, pb + a.si.length], si: b.si }],
            ];
          }
          return [
            [{ p: [key, pa + b.si.length], si: a.si }],
            [{ p: [key, pb], si: b.si }],
          ];
        }
      }
      return [origTransform(left, right, 'left'), origTransform(right, left, 'right')];
    };
    fastTransformX[FAST_TRANSFORM_X] = true;
    defaultType.transformX = fastTransformX;
  }
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
  private seq = 0;
  private unexpectedDisconnects = 0;

  constructor(
    readonly clientId: string,
    readonly roomId: string,
    private readonly gatewayUrls: readonly string[],
    private readonly random: Random,
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
      else {
        this.counters.skippedWhileOffline++;
        if (this.inWindow(scheduledAtMs)) {
          this.fanout?.noteScheduled(this.skipKey(scheduledAtMs), this.expectedRecipients);
        }
      }
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

  get unexpectedDisconnectCount(): number {
    return this.unexpectedDisconnects;
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
    this.sessionReady = false;
    this.dropSocket();
    this.connection?.close();
  }

  private text(): string {
    const text = this.doc?.data?.text;
    return typeof text === 'string' ? text : '';
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

  private send(scheduledAtMs: number): void {
    const doc = this.doc;
    if (!doc?.type || typeof doc.data?.text !== 'string') {
      this.counters.skippedWhileOffline++;
      if (this.inWindow(scheduledAtMs)) {
        this.fanout?.noteScheduled(this.skipKey(scheduledAtMs), this.expectedRecipients);
      }
      return;
    }
    const sentAtMs = preciseNowMs();
    this.recorder.recordScheduleLag(sentAtMs - scheduledAtMs);
    const position = Math.floor(this.random() * (doc.data.text.length + 1));
    const ch = ALPHABET[Math.floor(this.random() * ALPHABET.length)];
    const seq = ++this.seq;
    doc.submitOp([{ p: ['text', position], si: ch }], {
      source: { scheduledAtMs, sentAtMs, clientId: this.clientId, seq },
    }, (error) => {
      if (error) console.error(`ot submit ${this.clientId}`, error);
    });
    this.counters.sent++;
    if (this.inWindow(scheduledAtMs)) {
      this.counters.sentInWindow++;
      this.fanout?.noteScheduled(this.eventKey(this.clientId, seq), this.expectedRecipients);
    }
  }

  private onRemote(meta: OpMeta): void {
    if (meta.clientId === this.clientId) return;
    if (!this.live) {
      this.counters.catchUpEvents++;
      return;
    }
    this.counters.received++;
    if (!this.inWindow(meta.scheduledAtMs)) return;
    this.fanout?.noteDelivered(this.eventKey(meta.clientId, meta.seq), this.recipientIndex);
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

  private handleDocError(error: unknown): void {
    const code = (error as { code?: string } | null)?.code;
    if (code === 'ERR_DOC_ALREADY_CREATED' && (!this.sessionReady || Boolean(this.doc?.type))) {
      return;
    }
    console.error(`ot doc ${this.clientId}`, error);
  }

  private async connect(): Promise<void> {
    this.sessionReady = false;
    this.live = false;
    if (this.closed || this.intentionalDisconnect) return;
    await reconnectGate.acquire();
    let ws: WebSocket;
    try {
      if (this.closed || this.intentionalDisconnect) return;
      ws = new WebSocket(sharedbUrl(this.gatewayUrls[this.failover.index]));
      // Keep an 'error' listener attached from construction through close so
      // abortHandshake on a CONNECTING socket can never crash the process.
      ws.on('error', (error) => {
        this.lastConnectError = error;
      });
      this.ws = ws;
      ws.on('close', () => {
        if (ws === this.ws) this.onDisconnect();
      });
      await waitUntilOpen(ws, CONNECT_TIMEOUT_MS);
      if (this.closed || this.intentionalDisconnect) {
        this.dropSocket();
        return;
      }
      const socket = browserSocket(ws);
      if (!this.connection) {
        this.connection = new ShareDBClient.Connection(socket) as ShareConnection;
        this.connection.on('error', (error) => {
          console.error(`ot connection ${this.clientId}`, error);
        });
        this.doc = this.connection.get(OT_COLLECTION, this.roomId);
        this.doc.preventCompose = true;
        this.doc.submitSource = true;
        this.doc.on('error', (error) => {
          this.handleDocError(error);
        });
        this.doc.benchOnRemote = (meta) => this.onRemote(meta);
      } else {
        this.connection.bindToSocket(socket);
      }
    } catch (error) {
      this.dropSocket();
      throw error;
    } finally {
      reconnectGate.release();
    }
    try {
      await this.waitSubscribed(ws);
    } catch (error) {
      this.dropSocket();
      throw error;
    }
    if (this.closed || this.intentionalDisconnect) {
      this.dropSocket();
      return;
    }
    this.online = true;
    this.sessionReady = true;
    this.live = true;
  }

  private waitSubscribed(ws: WebSocket): Promise<void> {
    const doc = this.doc;
    if (!doc) return Promise.reject(new Error('connect timeout'));
    return new Promise((resolve, reject) => {
      let settled = false;
      let createFallbackTimer: NodeJS.Timeout | null = null;
      const cleanup = (): void => {
        clearTimeout(timer);
        if (createFallbackTimer) clearTimeout(createFallbackTimer);
        ws.off('close', onSocketClose);
        doc.off('create', onDocCreate);
      };
      const fail = (error: Error): void => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(error);
      };
      const ready = (): void => {
        if (settled) return;
        settled = true;
        cleanup();
        // History from this subscribe was applied before the callback. Later ops are live.
        this.live = true;
        resolve();
      };
      const onSocketClose = (): void => {
        fail(new Error('socket closed before subscribe completed'));
      };
      const onDocCreate = (): void => {
        if (doc.type) ready();
      };
      const timer = setTimeout(() => fail(new Error('connect timeout')), CONNECT_TIMEOUT_MS);
      ws.once('close', onSocketClose);

      const createWithFetchFallback = (): void => {
        if (settled) return;
        doc.create({ text: '' }, (createErr) => {
          if (settled) return;
          if (!createErr || doc.type) return ready();
          doc.fetch((fetchErr) => (fetchErr || !doc.type ? fail(fetchErr ?? createErr) : ready()));
        });
      };

      doc.subscribe((err) => {
        if (settled) return;
        if (err) return fail(err);
        if (doc.type) return ready();
        if (this.recipientIndex === 0) {
          createWithFetchFallback();
          return;
        }
        // Only one client per room creates eagerly; peers receive the create op
        // on their live subscription, with a fetch/create fallback if needed.
        doc.once('create', onDocCreate);
        createFallbackTimer = setTimeout(() => {
          if (settled) return;
          if (doc.type) return ready();
          doc.fetch((fetchErr) => {
            if (settled) return;
            if (!fetchErr && doc.type) return ready();
            createWithFetchFallback();
          });
        }, CREATE_FALLBACK_MS);
      });
    });
  }

  private dropSocket(): void {
    const socket = this.ws;
    this.ws = null;
    const share = this.connection?.socket;
    if (share) {
      const onClose = share.onclose;
      const wasClosed = share.readyState === WebSocket.CLOSED;
      // Detach handlers before notifying ShareDB of disconnect so late frames
      // on the closing WebSocket cannot reach Doc._handleSubscribe after
      // inflightSubscribe has been cleared.
      share.onmessage = null;
      share.onopen = null;
      share.onerror = null;
      share.onclose = null;
      share.readyState = WebSocket.CLOSED;
      if (!wasClosed) onClose?.('disconnected');
    }
    if (socket && socket.readyState !== WebSocket.CLOSED) socket.close();
  }

  private onDisconnect(): void {
    const droppedLiveSession = this.sessionReady;
    this.online = false;
    this.live = false;
    this.sessionReady = false;
    if (this.intentionalDisconnect || this.closed) return;
    this.unexpectedDisconnects++;
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
