import { latencyOf } from '@domain/latency';
import { ConnectGate, initialFailover, noteFailure, pickGateway } from '../../apps/load-generator/gateway-failover';
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

  it('keeps replacing reservoir samples after the cap (Algorithm R)', () => {
    const recorder = new LatencyRecorder(() => 0, 2);
    for (let i = 1; i <= 5; i++) {
      expect(recorder.shouldCapture()).toBe(true);
      recorder.recordServiceUs(i * 1000, latencyOf('r', i * 1000, i * 1000 + 10, undefined, i * 1000));
    }
    const summary = recorder.summary();
    expect(summary.count).toBe(5);
    expect(summary.reservoirMaxScheduledMs).toBe(5000);
    expect(summary.reservoirMinScheduledMs).toBe(2000);
  });
});
