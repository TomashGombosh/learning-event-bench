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
      { roomId: 'r1', replicas: 2, diverged: false, lostClients: 0, notConvergedClients: 0 },
      { roomId: 'r2', replicas: 2, diverged: true, lostClients: 0, notConvergedClients: 2 },
    ]);
    expect(reliability(outcomes)).toBe(0.5);
  });

  it('counts a lost client as not converged even when the other digests match', () => {
    const outcomes = evaluateRooms([
      { roomId: 'r1', clientId: 'a', digest: 'x' },
      { roomId: 'r1', clientId: 'b', digest: 'x' },
      { roomId: 'r1', clientId: 'c', digest: 'x' },
    ], new Set(['c']));
    expect(outcomes).toEqual([
      { roomId: 'r1', replicas: 3, diverged: true, lostClients: 1, notConvergedClients: 1 },
    ]);
    expect(reliability(outcomes)).toBe(0);
  });

  it('counts only the minority digest as not converged when a majority exists', () => {
    const outcomes = evaluateRooms([
      { roomId: 'r1', clientId: 'a', digest: 'x' },
      { roomId: 'r1', clientId: 'b', digest: 'x' },
      { roomId: 'r1', clientId: 'c', digest: 'y' },
    ]);
    expect(outcomes[0].diverged).toBe(true);
    expect(outcomes[0].notConvergedClients).toBe(1);
    expect(outcomes[0].lostClients).toBe(0);
  });

  it('refuses to compute R without rooms', () => {
    expect(() => reliability([])).toThrow();
  });

  it('decomposes latency into uplink and downlink', () => {
    expect(latencyOf('r', 100, 130, 110)).toEqual({ roomId: 'r', totalMs: 30, uplinkMs: 10, downlinkMs: 20 });
  });

  it('separates service latency from open-loop schedule lag', () => {
    expect(latencyOf('r', 100, 180, 160, 170)).toEqual({
      roomId: 'r', totalMs: 80, uplinkMs: 60, downlinkMs: 20, serviceMs: 10, scheduleLagMs: 70,
    });
  });
});
