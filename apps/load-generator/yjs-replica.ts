import { createHash } from 'node:crypto';
import * as Y from 'yjs';
import { Random } from './random';

const REMOTE = 'remote';
const ALPHABET = 'abcdefghijklmnopqrstuvwxyz ';

/** A Yjs document replica; local edits produce updates, remote updates are applied idempotently. */
export class YjsReplica {
  private readonly doc = new Y.Doc();
  private readonly text = this.doc.getText('code');
  private lastLocalUpdate: Uint8Array | null = null;
  /** Received payloads applied in digest(), not on the socket hot path. */
  private queued: string[] = [];

  constructor(private readonly random: Random) {
    this.doc.on('update', (update: Uint8Array, origin: unknown) => {
      if (origin !== REMOTE) this.lastLocalUpdate = update;
    });
  }

  /** Inserts one random character at a random position and returns the encoded update. */
  editRandomly(): string {
    const position = Math.floor(this.random() * (this.text.length + 1));
    const char = ALPHABET[Math.floor(this.random() * ALPHABET.length)];
    this.text.insert(position, char);
    const update = this.lastLocalUpdate!;
    this.lastLocalUpdate = null;
    return Buffer.from(update).toString('base64');
  }

  applyRemote(payloadBase64: string): void {
    Y.applyUpdate(this.doc, Buffer.from(payloadBase64, 'base64'), REMOTE);
  }

  /** Hold a remote update until digest(). Yjs apply is too expensive at fan-out rate. */
  queueRemote(payloadBase64: string): void {
    this.queued.push(payloadBase64);
  }

  /**
   * One-character Yjs updates, encoded up front. The measured document stays empty
   * so a skipped send is not already in the replica.
   */
  prebuild(count: number): string[] {
    const side = new YjsReplica(this.random);
    const pool = new Array<string>(count);
    for (let i = 0; i < count; i++) pool[i] = side.editRandomly();
    return pool;
  }

  /** CRDT bytes beyond the visible text. The op log equivalent lives inside the document. */
  stateStats(): { textBytes: number; stateBytes: number; metadataBytes: number } {
    this.flush();
    const text = this.text.toString();
    const textBytes = Buffer.byteLength(text);
    const stateBytes = Y.encodeStateAsUpdate(this.doc).byteLength;
    return { textBytes, stateBytes, metadataBytes: Math.max(0, stateBytes - textBytes) };
  }

  /** Digest of the visible state; replicas that missed an update end with a different digest. */
  digest(): string {
    this.flush();
    if (process.env.DEBUG_DIGEST) return `len=${this.text.length};pending=${this.doc.store.pendingStructs ? 1 : 0}`;
    return createHash('sha256').update(this.text.toString()).digest('hex');
  }

  private flush(): void {
    const queued = this.queued;
    if (queued.length === 0) return;
    this.queued = [];
    // One apply per update is minutes at L5000 (~25k updates/client). mergeUpdates
    // keeps the same document state; a dropped update still changes the digest.
    // 128 is faster than one-by-one and faster than larger merges (measured:
    // 50k updates 288 ms vs 485 ms sequential; a 4k chunk was slower).
    const CHUNK = 128;
    for (let i = 0; i < queued.length; i += CHUNK) {
      const merged = Y.mergeUpdates(
        queued.slice(i, i + CHUNK).map((payload) => Buffer.from(payload, 'base64')),
      );
      Y.applyUpdate(this.doc, merged, REMOTE);
    }
  }
}
