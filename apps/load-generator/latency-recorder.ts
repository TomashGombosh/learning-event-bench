import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import * as hdr from 'hdr-histogram-js';
import { LatencySample } from '@domain/latency';
import { Random } from './random';

const MICROS_PER_MS = 1000;
const PERCENTILES = [50, 75, 90, 95, 99, 99.9, 100];

/** HDR histogram for exact percentiles + a uniform reservoir of raw samples for bootstrap. */
export class LatencyRecorder {
  private readonly histogram = hdr.build({
    lowestDiscernibleValue: 1,
    highestTrackableValue: 3_600_000_000,
    numberOfSignificantValueDigits: 3,
  });
  private readonly reservoir: LatencySample[] = [];
  private seen = 0;

  constructor(private readonly random: Random, private readonly reservoirSize: number) {}

  record(sample: LatencySample): void {
    this.histogram.recordValue(Math.max(1, Math.round(sample.totalMs * MICROS_PER_MS)));
    this.seen++;
    if (this.reservoir.length < this.reservoirSize) this.reservoir.push(sample);
    else {
      const slot = Math.floor(this.random() * this.seen);
      if (slot < this.reservoirSize) this.reservoir[slot] = sample;
    }
  }

  get count(): number {
    return this.seen;
  }

  summary(): Record<string, number> {
    const result: Record<string, number> = { count: this.seen, meanMs: this.histogram.mean / MICROS_PER_MS };
    for (const p of PERCENTILES) result[`p${p}Ms`] = this.histogram.getValueAtPercentile(p) / MICROS_PER_MS;
    return result;
  }

  writeCsv(directory: string): void {
    mkdirSync(directory, { recursive: true });
    const summary = this.summary();
    writeFileSync(
      join(directory, 'latency_percentiles.csv'),
      'percentile,latency_ms\n' + PERCENTILES.map((p) => `${p},${summary[`p${p}Ms`]}`).join('\n') + '\n',
    );
    const rows = this.reservoir.map((s) =>
      [s.roomId, s.totalMs.toFixed(3), s.uplinkMs?.toFixed(3) ?? '', s.downlinkMs?.toFixed(3) ?? ''].join(','),
    );
    writeFileSync(join(directory, 'latency_samples.csv'), 'room,total_ms,uplink_ms,downlink_ms\n' + rows.join('\n') + '\n');
  }
}
