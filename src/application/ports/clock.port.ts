export interface ClockPort {
  nowMs(): number;
}

export const CLOCK = Symbol('ClockPort');

export const systemClock: ClockPort = { nowMs: () => Date.now() };
