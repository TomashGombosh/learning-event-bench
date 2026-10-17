/** Which gateway a client tries next. The killed URL stays out of the rotation until its cooldown ends. */

export interface FailoverState {
  /** Index into the gateway URL list. The first connection uses hash(clientId) % n. */
  index: number;
  /** Epoch ms. A gateway is skipped while `now < blockedUntil[i]`. */
  blockedUntil: number[];
}

export function hashCode(value: string): number {
  let hash = 0;
  for (let i = 0; i < value.length; i++) hash = (Math.imul(31, hash) + value.charCodeAt(i)) | 0;
  return hash;
}

export function initialFailover(clientId: string, gatewayCount: number): FailoverState {
  if (gatewayCount < 1) throw new Error('no gateway URLs');
  return {
    index: Math.abs(hashCode(clientId)) % gatewayCount,
    blockedUntil: Array.from({ length: gatewayCount }, () => 0),
  };
}

/** A later short cooldown must not clear a longer one (the killed gateway stays down). */
export function noteFailure(state: FailoverState, index: number, now: number, cooldownMs: number): void {
  const until = now + cooldownMs;
  if (until > state.blockedUntil[index]) state.blockedUntil[index] = until;
}

/**
 * The current index when it is usable, otherwise the next URL that is not cooling down.
 * Returns null when every gateway is blocked. Updates `state.index` to the choice.
 */
export function pickGateway(state: FailoverState, now: number): number | null {
  const n = state.blockedUntil.length;
  for (let step = 0; step < n; step++) {
    const index = (state.index + step) % n;
    if (state.blockedUntil[index] <= now) {
      state.index = index;
      return index;
    }
  }
  return null;
}

/** Caps concurrent handshakes so a gateway kill does not reconnect every client in one tick. */
export class ConnectGate {
  private active = 0;
  private readonly waiters: Array<() => void> = [];

  constructor(private readonly limit: number) {}

  async acquire(): Promise<void> {
    if (this.active >= this.limit) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    this.active++;
  }

  release(): void {
    this.active--;
    const next = this.waiters.shift();
    if (next) next();
  }
}
