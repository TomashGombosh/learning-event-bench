import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import * as hdr from 'hdr-histogram-js';
import { LatencySample } from '@domain/latency';
import { Random } from './random';

const MICROS_PER_MS = 1000;
const PERCENTILES = [50, 75, 90, 95, 99, 99.9, 100];

/** HDR histogram for exact percentiles + a uniform reservoir of raw samples for bootstrap. */
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

  constructor(private readonly random: Random, private readonly reservoirSize: number) {}

  /** Service latency of one delivered remote event (send -> receive). */
  record(sample: LatencySample): void {
    const serviceMs = sample.serviceMs ?? sample.totalMs;
    if (serviceMs < 0) this.clockSkew++;
    else this.service.recordValue(Math.max(1, Math.round(serviceMs * MICROS_PER_MS)));
    this.seen++;
    if (this.reservoir.length < this.reservoirSize) this.reservoir.push(sample);
    else {
      const slot = Math.floor(this.random() * this.seen);
      if (slot < this.reservoirSize) this.reservoir[slot] = sample;
    }
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
      ].join(',');
    });
    writeFileSync(
      join(directory, 'latency_samples.csv'),
      'room,total_ms,service_ms,schedule_lag_ms,uplink_ms,downlink_ms\n' + rows.join('\n') + '\n',
    );
  }

  private percentile(histogram: { getValueAtPercentile(p: number): number }, percentile: number, count: number): number {
    if (count === 0) return 0;
    return histogram.getValueAtPercentile(percentile) / MICROS_PER_MS;
  }
}
