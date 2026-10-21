/**
 * Per-event fanout of window sends, so a finite drain can be seen as censoring.
 *
 * A window event is one whose scheduled time is in [warmup, warmup + duration).
 * Recipients are the other clients in the room. At cutoff
 * C = warmup + duration + drain the process stops, and anything still unreceived
 * counts as never delivered.
 */
export interface CensoringCounts {
  /** Seconds from generation start to the end of the measurement window. */
  windowStartS: number;
  windowEndS: number;
  /** C = warmup + duration + drain. Deliveries after this are not observed. */
  cutoffS: number;
  windowEvents: number;
  /** Scheduled in the window and received by no recipient. Same count as deliveredToZero. */
  neverDelivered: number;
  deliveredToZero: number;
  deliveredToPartial: number;
  deliveredToAll: number;
}

interface FanoutSlot {
  expected: number;
  seen: number;
}

export class FanoutTracker {
  private readonly slots = new Map<string, FanoutSlot>();

  noteScheduled(key: string, expectedRecipients: number): void {
    if (this.slots.has(key)) return;
    this.slots.set(key, { expected: Math.max(0, expectedRecipients), seen: 0 });
  }

  /** One recipient index in `0 .. expected-1`. A repeat from the same recipient does not increase the count. */
  noteDelivered(key: string, recipientIndex: number): void {
    const slot = this.slots.get(key);
    if (!slot || recipientIndex < 0 || recipientIndex >= 31) return;
    slot.seen |= 1 << recipientIndex;
  }

  summarize(warmupS: number, durationS: number, drainS: number): CensoringCounts {
    let zero = 0;
    let partial = 0;
    let all = 0;
    for (const slot of this.slots.values()) {
      const got = bitCount(slot.seen);
      if (slot.expected <= 0 || got >= slot.expected) all += 1;
      else if (got === 0) zero += 1;
      else partial += 1;
    }
    return {
      windowStartS: warmupS,
      windowEndS: warmupS + durationS,
      cutoffS: warmupS + durationS + drainS,
      windowEvents: this.slots.size,
      neverDelivered: zero,
      deliveredToZero: zero,
      deliveredToPartial: partial,
      deliveredToAll: all,
    };
  }
}

function bitCount(value: number): number {
  let count = 0;
  let bits = value;
  while (bits) {
    count += bits & 1;
    bits >>>= 1;
  }
  return count;
}
