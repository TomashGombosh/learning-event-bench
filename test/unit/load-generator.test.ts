import { PoissonSchedule } from '../../apps/load-generator/poisson-schedule';
import { exponentialSeconds, seededRandom } from '../../apps/load-generator/random';
import { YjsReplica } from '../../apps/load-generator/yjs-replica';

describe('load generator building blocks', () => {
  it('produces exponential gaps with the requested mean', () => {
    const random = seededRandom(1);
    const n = 100_000;
    let sum = 0;
    for (let i = 0; i < n; i++) sum += exponentialSeconds(random, 5);
    expect(sum / n).toBeCloseTo(0.2, 2);
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

  it('Yjs updates are idempotent and order-independent (safe for catch-up duplicates)', () => {
    const a = new YjsReplica(seededRandom(6));
    const b = new YjsReplica(seededRandom(7));
    const updates = Array.from({ length: 10 }, () => a.editRandomly());
    [...updates].reverse().concat(updates).forEach((u) => b.applyRemote(u));
    expect(b.digest()).toBe(a.digest());
  });
});
