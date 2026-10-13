import { performance } from 'node:perf_hooks';

/** High-resolution wall-clock milliseconds shared by all simulated clients in this process. */
export const preciseNowMs = (): number => performance.timeOrigin + performance.now();
