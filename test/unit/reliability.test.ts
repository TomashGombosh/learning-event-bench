import { evaluateRooms, reliability } from '@domain/reliability';
import { latencyOf } from '@domain/latency';

describe('reliability', () => {
  it('marks a room diverged when replica digests differ', () => {
    const outcomes = evaluateRooms([
      { roomId: 'r1', clientId: 'a', digest: 'x' },
      { roomId: 'r1', clientId: 'b', digest: 'x' },
      { roomId: 'r2', clientId: 'a', digest: 'x' },
      { roomId: 'r2', clientId: 'b', digest: 'y' },
    ]);
    expect(outcomes).toEqual([
      { roomId: 'r1', replicas: 2, diverged: false },
      { roomId: 'r2', replicas: 2, diverged: true },
    ]);
    expect(reliability(outcomes)).toBe(0.5);
  });

  it('refuses to compute R without rooms', () => {
    expect(() => reliability([])).toThrow();
  });

  it('decomposes latency into uplink and downlink', () => {
    expect(latencyOf('r', 100, 130, 110)).toEqual({ roomId: 'r', totalMs: 30, uplinkMs: 10, downlinkMs: 20 });
  });
});
