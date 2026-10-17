import { exponentialSeconds, Random } from './random';

/**
 * Open-loop Poisson arrival schedule. The next arrival is computed from the previous
 * *scheduled* time, not from the time the send completed, which prevents coordinated omission.
 */
export class PoissonSchedule {
  private nextAtMs: number;

  constructor(
    private readonly random: Random,
    private readonly ratePerSecond: number,
    startAtMs: number,
  ) {
    this.nextAtMs = startAtMs + this.gapMs();
  }

  peek(): number {
    return this.nextAtMs;
  }

  advance(): number {
    const current = this.nextAtMs;
    this.nextAtMs += this.gapMs();
    return current;
  }

  /** Start a new open-loop schedule. Used once generation actually begins (after connect). */
  reset(startAtMs: number): void {
    this.nextAtMs = startAtMs + this.gapMs();
  }

  /**
   * Skip arrivals that fall before `nowMs` (used while a client is offline).
   * A schedule that was never armed used to start at 0, and walking every slot
   * up to the current epoch blocked the event loop. Cap the walk and jump.
   */
  skipUntil(nowMs: number): number {
    let skipped = 0;
    while (this.nextAtMs < nowMs && skipped < 100_000) {
      this.advance();
      skipped++;
    }
    if (this.nextAtMs < nowMs) this.nextAtMs = nowMs + this.gapMs();
    return skipped;
  }

  private gapMs(): number {
    return exponentialSeconds(this.random, this.ratePerSecond) * 1000;
  }
}
