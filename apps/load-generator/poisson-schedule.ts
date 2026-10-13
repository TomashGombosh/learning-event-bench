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

  /** Skip arrivals that fall before `nowMs` (used while a client is offline). */
  skipUntil(nowMs: number): number {
    let skipped = 0;
    while (this.nextAtMs < nowMs) {
      this.advance();
      skipped++;
    }
    return skipped;
  }

  private gapMs(): number {
    return exponentialSeconds(this.random, this.ratePerSecond) * 1000;
  }
}
