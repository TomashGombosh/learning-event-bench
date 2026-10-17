import { preciseNowMs } from './clock';

export interface DueClient {
  sendDue(nowMs: number): void;
}

/**
 * One timer for the whole shard. A timer per client (hundreds) plus a timer per
 * in-flight op ack was enough to pin the Node thread at a few hundred sends/s.
 */
export class ShardScheduler {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(private readonly clients: readonly DueClient[]) {}

  start(): void {
    this.running = true;
    const tick = (): void => {
      if (!this.running) return;
      const now = preciseNowMs();
      const clients = this.clients;
      for (let i = 0; i < clients.length; i++) clients[i].sendDue(now);
      this.timer = setTimeout(tick, 1);
    };
    tick();
  }

  stop(): void {
    this.running = false;
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
  }
}
