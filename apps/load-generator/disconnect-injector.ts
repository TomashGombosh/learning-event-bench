import { exponentialSeconds, Random } from './random';

export interface Disconnectable {
  disconnect(): void;
  reconnect(): Promise<void>;
  readonly isOnline: boolean;
}

export interface DisconnectPlan {
  /** nu: disconnects per client per minute. */
  readonly ratePerMinute: number;
  /** delta: mean offline duration (seconds), exponentially distributed. */
  readonly meanOfflineSeconds: number;
}

/** Injects Poisson-distributed client disconnects with exponential durations. */
export class DisconnectInjector {
  private readonly timers = new Set<NodeJS.Timeout>();
  private active = false;
  injected = 0;

  constructor(private readonly random: Random, private readonly plan: DisconnectPlan) {}

  start(clients: readonly Disconnectable[]): void {
    if (this.plan.ratePerMinute <= 0) return;
    this.active = true;
    clients.forEach((client) => this.scheduleNext(client));
  }

  async stopAndRestore(clients: readonly Disconnectable[]): Promise<void> {
    this.active = false;
    this.timers.forEach(clearTimeout);
    this.timers.clear();
    await Promise.all(clients.filter((c) => !c.isOnline).map((c) => c.reconnect()));
  }

  private scheduleNext(client: Disconnectable): void {
    const waitMs = exponentialSeconds(this.random, this.plan.ratePerMinute / 60) * 1000;
    this.later(waitMs, () => {
      if (!this.active || !client.isOnline) return this.scheduleNext(client);
      client.disconnect();
      this.injected++;
      const offlineMs = exponentialSeconds(this.random, 1 / this.plan.meanOfflineSeconds) * 1000;
      this.later(offlineMs, async () => {
        if (!this.active) return;
        await client.reconnect();
        this.scheduleNext(client);
      });
    });
  }

  private later(ms: number, fn: () => void | Promise<void>): void {
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      void fn();
    }, ms);
    this.timers.add(timer);
  }
}
