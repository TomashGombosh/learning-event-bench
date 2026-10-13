/** Seeded PRNG (mulberry32) so that every run is reproducible from its seed. */
export type Random = () => number;

export function seededRandom(seed: number): Random {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Exponential variate with the given rate (events per second) -> seconds. */
export function exponentialSeconds(random: Random, ratePerSecond: number): number {
  if (ratePerSecond <= 0) return Number.POSITIVE_INFINITY;
  return -Math.log(1 - random()) / ratePerSecond;
}
