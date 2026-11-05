import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import * as hdr from 'hdr-histogram-js';
import { LatencySample } from '@domain/latency';
import { Random } from './random';

const MICROS_PER_MS = 1000;
const PERCENTILES = [50, 75, 90, 95, 99, 99.9, 100];

/**
 * HDR histogram for exact percentiles, plus a bounded reservoir of raw samples.
 * The histogram records every delivery. The reservoir is Vitter/Li Algorithm L:
 * a uniform sample of the whole in-window stream, with a geometric skip so the
 * hot path is a counter compare once the reservoir is full.
 */
export class LatencyRecorder {
  private readonly service = hdr.build({
    lowestDiscernibleValue: 1,
    highestTrackableValue: 3_600_000_000,
    numberOfSignificantValueDigits: 3,
  });
  private readonly lag = hdr.build({
    lowestDiscernibleValue: 1,
    highestTrackableValue: 3_600_000_000,
    numberOfSignificantValueDigits: 3,
  });
  private readonly reservoir: LatencySample[] = [];
  private seen = 0;
  private lagSeen = 0;
  private clockSkew = 0;
  /** Slot chosen by the last shouldCapture() call. -1 means this delivery is histogram-only. */
  private pendingSlot = -1;
  /** Algorithm L threshold. The next 0-based index that enters the reservoir. */
  private nextAccept = Number.POSITIVE_INFINITY;
  /** Algorithm L weight. Inclusion gaps are geometric with success probability w. */
  private w = 1;
  private lArmed = false;

  constructor(private readonly random: Random, private readonly reservoirSize: number) {}

  /**
   * Whether this delivery should allocate a raw sample. Call once, then recordServiceUs.
   * The first `reservoirSize` deliveries fill the reservoir. Later deliveries are skipped
   * until the precomputed Algorithm L index, which replaces one uniform slot.
   */
  shouldCapture(): boolean {
    this.pendingSlot = this.chooseSlot();
    return this.pendingSlot >= 0;
  }

  /** Hot path: HDR histogram always. `sample` is set only when shouldCapture() was true. */
  recordServiceUs(serviceUs: number, sample?: LatencySample): void {
    if (serviceUs < 0) this.clockSkew++;
    else this.service.recordValue(Math.max(1, Math.round(serviceUs)));
    this.seen++;
    this.storePending(sample);
  }

  /** Service latency of one delivered remote event (send -> receive). Same reservoir as shouldCapture. */
  record(sample: LatencySample): void {
    const serviceMs = sample.serviceMs ?? sample.totalMs;
    const slot = this.chooseSlot();
    if (serviceMs < 0) this.clockSkew++;
    else this.service.recordValue(Math.max(1, Math.round(serviceMs * MICROS_PER_MS)));
    this.seen++;
    this.pendingSlot = slot;
    this.storePending(sample);
  }

  /** Open-loop delay between the scheduled send time and the actual send. */
  recordScheduleLag(lagMs: number): void {
    this.lag.recordValue(Math.max(1, Math.round(Math.max(0, lagMs) * MICROS_PER_MS)));
    this.lagSeen++;
  }

  get count(): number {
    return this.seen;
  }

  summary(): Record<string, number> {
    const result: Record<string, number> = {
      count: this.seen,
      meanMs: this.seen === 0 ? 0 : this.service.mean / MICROS_PER_MS,
      clockSkewSamples: this.clockSkew,
      scheduleLagCount: this.lagSeen,
      scheduleLagP50Ms: this.percentile(this.lag, 50, this.lagSeen),
      scheduleLagP99Ms: this.percentile(this.lag, 99, this.lagSeen),
      reservoirMinScheduledMs: this.reservoirSpan().min,
      reservoirMaxScheduledMs: this.reservoirSpan().max,
      reservoirEligible: this.seen,
      reservoirKept: this.reservoir.length,
    };
    for (const p of PERCENTILES) result[`p${p}Ms`] = this.percentile(this.service, p, this.seen);
    return result;
  }

  writeCsv(directory: string): void {
    mkdirSync(directory, { recursive: true });
    const summary = this.summary();
    writeFileSync(
      join(directory, 'latency_percentiles.csv'),
      'percentile,latency_ms\n' + PERCENTILES.map((p) => `${p},${summary[`p${p}Ms`]}`).join('\n') + '\n',
    );
    const rows = this.reservoir.map((s) => {
      const service = s.serviceMs ?? s.totalMs;
      return [
        s.roomId,
        s.totalMs.toFixed(3),
        service.toFixed(3),
        s.scheduleLagMs?.toFixed(3) ?? '',
        s.uplinkMs?.toFixed(3) ?? '',
        s.downlinkMs?.toFixed(3) ?? '',
        s.scheduledAtMs?.toFixed(3) ?? '',
      ].join(',');
    });
    writeFileSync(
      join(directory, 'latency_samples.csv'),
      'room,total_ms,service_ms,schedule_lag_ms,uplink_ms,downlink_ms,scheduled_at_ms\n' + rows.join('\n') + '\n',
    );
  }

  /** Open (0, 1). log(0) and a draw of 1 would stall or overshoot Algorithm L. */
  private unit(): number {
    const u = this.random();
    if (!(u > 0)) return Number.MIN_VALUE;
    if (u >= 1) return 1 - Number.EPSILON;
    return u;
  }

  /** Keep 1-w inside (0, 1) so the geometric skip stays finite. */
  private clampW(w: number): number {
    if (!(w > 0) || !(w < 1)) return 0.5;
    if (1 - w === 0) return 1 - Number.EPSILON;
    if (1 - w === 1) return Number.EPSILON;
    return w;
  }

  /** Records to skip before the next acceptance. Does not update w. */
  private geometricSkip(): number {
    const skip = Math.floor(Math.log(this.unit()) / Math.log(1 - this.w));
    return Number.isFinite(skip) && skip >= 0 ? skip : 0;
  }

  /**
   * Arm Algorithm L once the reservoir holds k items. The next candidate is
   * 0-based index k (1-based k+1). Wikipedia's loop leaves i at the last filled
   * item and then adds floor(log(u)/log(1-W))+1, which lands on k+skip.
   */
  private armL(): void {
    this.w = this.clampW(Math.exp(Math.log(this.unit()) / this.reservoirSize));
    this.nextAccept = this.reservoirSize + this.geometricSkip();
    this.lArmed = true;
  }

  /** 0-based index of the item about to be counted. -1 means histogram only. */
  private chooseSlot(): number {
    if (!(this.reservoirSize > 0)) return -1;
    const index = this.seen;
    if (this.reservoir.length < this.reservoirSize) return this.reservoir.length;
    if (!this.lArmed) this.armL();
    if (index !== this.nextAccept) return -1;
    const slot = Math.floor(this.unit() * this.reservoirSize);
    this.w = this.clampW(this.w * Math.exp(Math.log(this.unit()) / this.reservoirSize));
    this.nextAccept = index + 1 + this.geometricSkip();
    return slot;
  }

  private storePending(sample: LatencySample | undefined): void {
    const slot = this.pendingSlot;
    this.pendingSlot = -1;
    if (sample === undefined || slot < 0 || slot > this.reservoir.length) return;
    if (slot === this.reservoir.length) this.reservoir.push(sample);
    else this.reservoir[slot] = sample;
  }

  private reservoirSpan(): { min: number; max: number } {
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    for (const sample of this.reservoir) {
      if (sample.scheduledAtMs === undefined) continue;
      if (sample.scheduledAtMs < min) min = sample.scheduledAtMs;
      if (sample.scheduledAtMs > max) max = sample.scheduledAtMs;
    }
    if (!Number.isFinite(min)) return { min: 0, max: 0 };
    return { min, max };
  }

  private percentile(histogram: { getValueAtPercentile(p: number): number }, percentile: number, count: number): number {
    if (count === 0) return 0;
    return histogram.getValueAtPercentile(percentile) / MICROS_PER_MS;
  }
}
