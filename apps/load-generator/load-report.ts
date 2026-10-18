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
}): LoadAssessment {
  const reasons: string[] = [];
  if (input.scheduleLagP50Ms > 250) {
    reasons.push(`generator schedule lag p50 ${Math.round(input.scheduleLagP50Ms)}ms`);
  }
  // Fault injection used to exempt a short rate. A cell that delivers under 95% of the
  // offered load is saturated either way: the broker (or the generator) did not keep up.
  if (input.offeredPerSec > 0 && input.achievedSendPerSec < 0.95 * input.offeredPerSec) {
    reasons.push(
      `achieved send ${input.achievedSendPerSec.toFixed(1)}/s is under 95% of offered ${input.offeredPerSec.toFixed(1)}/s`,
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
