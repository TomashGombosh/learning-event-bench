export interface LoadAssessment {
  readonly offeredPerSec: number;
  readonly achievedSendPerSec: number;
  readonly achievedDeliveryPerSec: number;
  readonly scheduleLagP50Ms: number;
  readonly serviceP50Ms: number;
  readonly saturated: boolean;
  /** Null when the shard kept up with the offered rate. */
  readonly saturationReason: string | null;
}

/**
 * Minute-scale p50 is generator queueing or broker overload, not a silent latency sample.
 * Schedule lag is reported beside service time; it does not replace it.
 */
export function assessLoad(input: {
  readonly offeredPerSec: number;
  readonly achievedSendPerSec: number;
  readonly achievedDeliveryPerSec: number;
  readonly scheduleLagP50Ms: number;
  readonly serviceP50Ms: number;
  /** Disconnects lower the send rate without the generator being CPU-bound. */
  readonly disconnectsPerMin: number;
}): LoadAssessment {
  const reasons: string[] = [];
  if (input.scheduleLagP50Ms > 250) {
    reasons.push(`generator schedule lag p50 ${Math.round(input.scheduleLagP50Ms)}ms`);
  }
  if (input.disconnectsPerMin <= 0 && input.offeredPerSec > 0 && input.achievedSendPerSec < 0.8 * input.offeredPerSec) {
    reasons.push(
      `achieved send ${input.achievedSendPerSec.toFixed(1)}/s vs offered ${input.offeredPerSec.toFixed(1)}/s`,
    );
  }
  if (input.serviceP50Ms > 5_000) {
    reasons.push(`service p50 ${Math.round(input.serviceP50Ms)}ms`);
  }
  return {
    offeredPerSec: input.offeredPerSec,
    achievedSendPerSec: input.achievedSendPerSec,
    achievedDeliveryPerSec: input.achievedDeliveryPerSec,
    scheduleLagP50Ms: input.scheduleLagP50Ms,
    serviceP50Ms: input.serviceP50Ms,
    saturated: reasons.length > 0,
    saturationReason: reasons.length > 0 ? reasons.join('; ') : null,
  };
}
