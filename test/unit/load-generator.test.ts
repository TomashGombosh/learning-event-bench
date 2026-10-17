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
      scheduleLagP50Ms: 90_000, serviceP50Ms: 40, disconnectsPerMin: 0,
    });
    expect(behind.saturated).toBe(true);
    expect(behind.saturationReason).toContain('schedule lag');
    expect(behind.saturationReason).toContain('offered');
    const healthy = assessLoad({
      offeredPerSec: 200, achievedSendPerSec: 198, achievedDeliveryPerSec: 3700,
      scheduleLagP50Ms: 4, serviceP50Ms: 12, disconnectsPerMin: 0,
    });
    expect(healthy.saturated).toBe(false);
    expect(healthy.saturationReason).toBeNull();
  });

  it('does not treat fault-injected disconnects as a missed offered rate', () => {
    const faults = assessLoad({
      offeredPerSec: 5000, achievedSendPerSec: 1000, achievedDeliveryPerSec: 2000,
      scheduleLagP50Ms: 8, serviceP50Ms: 20, disconnectsPerMin: 1,
    });
    expect(faults.saturated).toBe(false);
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

  it('Yjs updates are idempotent and order-independent (safe for catch-up duplicates)', () => {
    const a = new YjsReplica(seededRandom(6));
    const b = new YjsReplica(seededRandom(7));
    const updates = Array.from({ length: 10 }, () => a.editRandomly());
    [...updates].reverse().concat(updates).forEach((u) => b.applyRemote(u));
    expect(b.digest()).toBe(a.digest());
  });
});
