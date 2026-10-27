import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { latencyOf } from '@domain/latency';
import { ConnectGate, initialFailover, noteFailure, pickGateway } from '../../apps/load-generator/gateway-failover';
import { FanoutTracker } from '../../apps/load-generator/fanout-tracker';
import { LatencyRecorder } from '../../apps/load-generator/latency-recorder';
import { assessLoad } from '../../apps/load-generator/load-report';
import { PoissonSchedule } from '../../apps/load-generator/poisson-schedule';
import { exponentialSeconds, seededRandom } from '../../apps/load-generator/random';
import { ShardScheduler } from '../../apps/load-generator/shard-scheduler';
import { YjsReplica } from '../../apps/load-generator/yjs-replica';

describe('load generator building blocks', () => {
  it('produces exponential gaps with the requested mean', () => {
    const random = seededRandom(1);
    const n = 100_000;
    let sum = 0;
    for (let i = 0; i < n; i++) sum += exponentialSeconds(random, 5);
    expect(sum / n).toBeCloseTo(0.2, 2);
  });

  it('skipUntil jumps a schedule that starts at the epoch instead of walking it', () => {
    const schedule = new PoissonSchedule(seededRandom(2), 10, 0);
    const started = Date.now();
    schedule.skipUntil(1e15);
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(schedule.peek()).toBeGreaterThan(1e15);
  });

  it('schedules arrivals open-loop from previous scheduled time', () => {
    const schedule = new PoissonSchedule(seededRandom(2), 10, 0);
    const arrivals = Array.from({ length: 10_000 }, () => schedule.advance());
    expect(arrivals[arrivals.length - 1] / arrivals.length).toBeCloseTo(100, -1);
    expect(arrivals.every((t, i) => i === 0 || t >= arrivals[i - 1])).toBe(true);
  });

  it('Yjs replicas converge when all updates are delivered and diverge when one is lost', () => {
    const a = new YjsReplica(seededRandom(3));
    const b = new YjsReplica(seededRandom(4));
    const c = new YjsReplica(seededRandom(5));
    const updates = Array.from({ length: 20 }, () => a.editRandomly());
    updates.forEach((u) => b.applyRemote(u));
    updates.filter((_, i) => i !== 5).forEach((u) => c.applyRemote(u));
    expect(b.digest()).toBe(a.digest());
    expect(c.digest()).not.toBe(a.digest());
  });

  it('flags generator lag and a missed offered rate as saturation', () => {
    const behind = assessLoad({
      offeredPerSec: 5000, achievedSendPerSec: 800, achievedDeliveryPerSec: 15000,
      scheduleLagP50Ms: 90_000, serviceP50Ms: 40,
    });
    expect(behind.saturated).toBe(true);
    expect(behind.saturationReason).toContain('schedule lag');
    expect(behind.saturationReason).toContain('95%');
    const healthy = assessLoad({
      offeredPerSec: 200, achievedSendPerSec: 198, achievedDeliveryPerSec: 3700,
      scheduleLagP50Ms: 4, serviceP50Ms: 12,
    });
    expect(healthy.saturated).toBe(false);
    expect(healthy.saturationReason).toBeNull();
  });

  it('counts offline skips as attempted sends, not as saturation', () => {
    const faults = assessLoad({
      offeredPerSec: 5000, achievedSendPerSec: 1000, attemptedPerSec: 4900, achievedDeliveryPerSec: 2000,
      scheduleLagP50Ms: 8, serviceP50Ms: 20,
    });
    expect(faults.saturated).toBe(false);
    expect(faults.clientsOffline).toBe(true);
    expect(faults.offlineFraction).toBeGreaterThan(0.05);
    const stillShort = assessLoad({
      offeredPerSec: 5000, achievedSendPerSec: 1000, attemptedPerSec: 4000, achievedDeliveryPerSec: 2000,
      scheduleLagP50Ms: 8, serviceP50Ms: 20,
    });
    expect(stillShort.saturated).toBe(true);
    expect(stillShort.saturationReason).toContain('attempted');
    const keptUp = assessLoad({
      offeredPerSec: 5000, achievedSendPerSec: 4800, achievedDeliveryPerSec: 4700,
      scheduleLagP50Ms: 8, serviceP50Ms: 20,
    });
    expect(keptUp.saturated).toBe(false);
    expect(keptUp.clientsOffline).toBe(false);
  });

  it('queued prebuilt updates converge and a missing update diverges', () => {
    const pool = new YjsReplica(seededRandom(8)).prebuild(25);
    const a = new YjsReplica(seededRandom(9));
    const b = new YjsReplica(seededRandom(10));
    const c = new YjsReplica(seededRandom(11));
    pool.forEach((payload) => {
      a.queueRemote(payload);
      b.queueRemote(payload);
    });
    pool.filter((_, index) => index !== 4).forEach((payload) => c.queueRemote(payload));
    expect(pool.every((payload) => payload.length > 0)).toBe(true);
    expect(b.digest()).toBe(a.digest());
    expect(c.digest()).not.toBe(a.digest());
  });

  it('merged remote updates match sequential apply across the chunk boundary', () => {
    const pool = new YjsReplica(seededRandom(12)).prebuild(600);
    const merged = new YjsReplica(seededRandom(13));
    const sequential = new YjsReplica(seededRandom(14));
    pool.forEach((payload) => {
      merged.queueRemote(payload);
      sequential.applyRemote(payload);
    });
    expect(merged.digest()).toBe(sequential.digest());
  });

  it('one scheduler tick drives every client', async () => {
    let calls = 0;
    const scheduler = new ShardScheduler([
      { sendDue() { calls += 1; } },
      { sendDue() { calls += 1; } },
    ]);
    scheduler.start();
    await new Promise((resolve) => setTimeout(resolve, 40));
    scheduler.stop();
    expect(calls).toBeGreaterThanOrEqual(4);
  });

  it('skips a killed gateway instead of hashing those clients back onto it', () => {
    // Smoke cell 005: these six clients logged `reconnect gave up` and all hash to gateway2 (index 1).
    for (const clientId of ['c1-9', 'c40-6', 'c9-13', 'c34-6', 'c2-14', 'c46-14']) {
      const state = initialFailover(clientId, 3);
      expect(state.index).toBe(1);
      noteFailure(state, 1, 1_000, 30_000);
      state.index = 2;
      expect(pickGateway(state, 1_000)).toBe(2);
      noteFailure(state, 2, 1_200, 1_000);
      expect(pickGateway(state, 1_200)).toBe(0);
      expect(pickGateway(state, 1_200)).toBe(0);
      expect(pickGateway(state, 31_000)).toBe(0);
      state.index = 1;
      expect(pickGateway(state, 31_000)).toBe(1);
    }
  });

  it('waits when every gateway is cooling down', () => {
    const state = initialFailover('c1-9', 3);
    noteFailure(state, 0, 0, 5_000);
    noteFailure(state, 1, 0, 5_000);
    noteFailure(state, 2, 0, 5_000);
    expect(pickGateway(state, 1_000)).toBeNull();
    expect(pickGateway(state, 5_000)).toBe(state.index);
  });

  it('caps concurrent connection attempts', async () => {
    const gate = new ConnectGate(2);
    let active = 0;
    let max = 0;
    await Promise.all(Array.from({ length: 8 }, async () => {
      await gate.acquire();
      active += 1;
      max = Math.max(max, active);
      await new Promise((resolve) => setTimeout(resolve, 30));
      active -= 1;
      gate.release();
    }));
    expect(max).toBeLessThanOrEqual(2);
    expect(max).toBe(2);
  });

  it('Yjs updates are idempotent and order-independent (safe for catch-up duplicates)', () => {
    const a = new YjsReplica(seededRandom(6));
    const b = new YjsReplica(seededRandom(7));
    const updates = Array.from({ length: 10 }, () => a.editRandomly());
    [...updates].reverse().concat(updates).forEach((u) => b.applyRemote(u));
    expect(b.digest()).toBe(a.digest());
  });

  it('counts window events delivered to none, some, and all recipients', () => {
    const fanout = new FanoutTracker();
    fanout.noteScheduled('none', 2);
    fanout.noteScheduled('partial', 2);
    fanout.noteDelivered('partial', 0);
    fanout.noteScheduled('all', 2);
    fanout.noteDelivered('all', 0);
    fanout.noteDelivered('all', 1);
    fanout.noteDelivered('all', 1);
    const counts = fanout.summarize(30, 240, 15);
    expect(counts.windowStartS).toBe(30);
    expect(counts.windowEndS).toBe(270);
    expect(counts.cutoffS).toBe(285);
    expect(counts.windowEvents).toBe(3);
    expect(counts.neverDelivered).toBe(1);
    expect(counts.deliveredToZero).toBe(1);
    expect(counts.deliveredToPartial).toBe(1);
    expect(counts.deliveredToAll).toBe(1);
  });

  it('caps the reservoir and skips with Algorithm L instead of a draw per delivery', () => {
    const n = 10_000;
    const k = 100;
    let draws = 0;
    const random = seededRandom(20261021);
    const recorder = new LatencyRecorder(() => {
      draws++;
      return random();
    }, k);
    let captures = 0;
    for (let i = 0; i < n; i++) {
      if (recorder.shouldCapture()) {
        captures++;
        recorder.recordServiceUs((i + 1) * 1000, latencyOf('r', i, i + 1, undefined, i));
      } else {
        recorder.recordServiceUs((i + 1) * 1000);
      }
    }
    const summary = recorder.summary();
    expect(summary.count).toBe(n);
    expect(summary.reservoirEligible).toBe(n);
    expect(summary.reservoirKept).toBe(k);
    expect(captures).toBeGreaterThan(k);
    expect(captures).toBeLessThan(n / 2);
    // Fill uses no draws. Arming uses 2; each later acceptance uses 3.
    expect(draws).toBe(2 + 3 * (captures - k));
  });

  it('spreads raw samples uniformly across the record window', () => {
    const n = 20_000;
    const k = 2_000;
    const sampleCsv = (seed: number): string => {
      const recorder = new LatencyRecorder(seededRandom(seed), k);
      for (let i = 0; i < n; i++) {
        const serviceMs = i;
        if (recorder.shouldCapture()) {
          recorder.recordServiceUs(serviceMs * 1000, latencyOf('r', i, i + serviceMs, undefined, i));
        } else {
          recorder.recordServiceUs(serviceMs * 1000);
        }
      }
      const dir = mkdtempSync(join(tmpdir(), 'reservoir-'));
      try {
        recorder.writeCsv(dir);
        return readFileSync(join(dir, 'latency_samples.csv'), 'utf8');
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    };
    const csv = sampleCsv(20261021);
    expect(sampleCsv(20261021)).toBe(csv);
    const lines = csv.trim().split('\n');
    expect(lines[0]).toBe('room,total_ms,service_ms,schedule_lag_ms,uplink_ms,downlink_ms,scheduled_at_ms');
    const scheduled = lines.slice(1).map((line) => Number(line.split(',')[6]));
    const service = lines.slice(1).map((line) => Number(line.split(',')[2]));
    expect(scheduled).toHaveLength(k);
    const bins = [0, 0, 0, 0];
    const width = n / bins.length;
    for (const t of scheduled) bins[Math.min(bins.length - 1, Math.floor(t / width))]++;
    const expected = k / bins.length;
    for (const count of bins) {
      expect(count).toBeGreaterThan(expected * 0.5);
      expect(count).toBeLessThan(expected * 1.5);
    }
    const meanService = service.reduce((sum, value) => sum + value, 0) / service.length;
    expect(meanService).toBeGreaterThan(n * 0.4);
    expect(meanService).toBeLessThan(n * 0.6);
    expect(Math.min(...scheduled)).toBeLessThan(0.1 * (n - 1));
    expect(Math.max(...scheduled)).toBeGreaterThan(0.9 * (n - 1));
  });

  it('keeps every in-window delivery in the HDR histogram while the reservoir stays capped', () => {
    const n = 4_000;
    const recorder = new LatencyRecorder(seededRandom(7), 100);
    for (let i = 0; i < n; i++) {
      const serviceUs = i < n / 2 ? 1_000 : 100_000;
      if (recorder.shouldCapture()) recorder.recordServiceUs(serviceUs, latencyOf('r', i, i + serviceUs / 1000, undefined, i));
      else recorder.recordServiceUs(serviceUs);
    }
    const summary = recorder.summary();
    expect(summary.count).toBe(n);
    expect(summary.reservoirKept).toBe(100);
    expect(summary.reservoirEligible).toBe(n);
    expect(summary.meanMs).toBeGreaterThan(40);
    expect(summary.meanMs).toBeLessThan(60);
    expect(summary.p100Ms).toBeGreaterThan(50);
  });

  it('record() keeps the same cap and covers the whole stream', () => {
    const n = 5_000;
    const k = 200;
    const recorder = new LatencyRecorder(seededRandom(20261021), k);
    for (let i = 0; i < n; i++) recorder.record(latencyOf('r', i, i + i, undefined, i));
    const summary = recorder.summary();
    expect(summary.count).toBe(n);
    expect(summary.reservoirEligible).toBe(n);
    expect(summary.reservoirKept).toBe(k);
    expect(summary.reservoirMinScheduledMs).toBeLessThan(0.1 * (n - 1));
    expect(summary.reservoirMaxScheduledMs).toBeGreaterThan(0.9 * (n - 1));
  });

  it('includes early and late deliveries at the same rate', () => {
    const n = 80;
    const k = 8;
    const trials = 300;
    const hits = new Array<number>(n).fill(0);
    for (let trial = 1; trial <= trials; trial++) {
      const recorder = new LatencyRecorder(seededRandom(trial), k);
      for (let i = 0; i < n; i++) {
        if (recorder.shouldCapture()) recorder.recordServiceUs(1000, latencyOf('r', i, i + 1, undefined, i));
        else recorder.recordServiceUs(1000);
      }
      const dir = mkdtempSync(join(tmpdir(), 'reservoir-'));
      try {
        recorder.writeCsv(dir);
        const csv = readFileSync(join(dir, 'latency_samples.csv'), 'utf8');
        for (const line of csv.trim().split('\n').slice(1)) hits[Number(line.split(',')[6])]++;
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
      expect(recorder.summary().reservoirKept).toBe(k);
    }
    const expected = trials * (k / n);
    for (const index of [0, 20, 40, 60, n - 1]) {
      expect(hits[index]).toBeGreaterThan(expected * 0.45);
      expect(hits[index]).toBeLessThan(expected * 1.7);
    }
  });
});
