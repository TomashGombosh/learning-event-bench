const Agent = require('sharedb/lib/agent');

const PATCHED = Symbol.for('leb.sharedb.forwardOpMeta');

/**
 * ShareDB stores `x` on the committed op and publishes it, then drops it in
 * `Agent._sendOp`. Timing for the benchmark rides in `x.source`. Put it back
 * on the wire so a remote client can compute T = received − scheduled.
 */
export function forwardOpMeta(): void {
  if (Agent.prototype[PATCHED]) return;
  const original = Agent.prototype._sendOp;
  Agent.prototype._sendOp = function(this: { send: (message: Record<string, unknown>) => void },
    collection: string, id: string, op: { x?: unknown },
  ): void {
    const send = this.send;
    this.send = function(this: { send: (message: Record<string, unknown>) => void }, message: Record<string, unknown>) {
      this.send = send;
      if (message && message.a === 'op' && op && op.x) message.x = op.x;
      return send.call(this, message);
    };
    try {
      original.call(this, collection, id, op);
    } finally {
      this.send = send;
    }
  };
  Agent.prototype[PATCHED] = true;
}
