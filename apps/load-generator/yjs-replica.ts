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

  /** Digest of the visible state; replicas that missed an update end with a different digest. */
  digest(): string {
    if (process.env.DEBUG_DIGEST) return `len=${this.text.length};pending=${this.doc.store.pendingStructs ? 1 : 0}`;
    return createHash('sha256').update(this.text.toString()).digest('hex');
  }
}
