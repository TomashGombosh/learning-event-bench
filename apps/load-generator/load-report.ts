export interface LoadAssessment {
  readonly offeredPerSec: number;
  readonly achievedSendPerSec: number;
  readonly attemptedPerSec: number;
  readonly achievedDeliveryPerSec: number;
  readonly scheduleLagP50Ms: number;
  readonly serviceP50Ms: number;
  /** skippedWhileOffline / attempted. Fault injection, not a short generator. */
  readonly offlineFraction: number;
  readonly clientsOffline: boolean;
  readonly saturated: boolean;
  /** Null when the shard kept up with the offered rate. */
  readonly saturationReason: string | null;
}

/**
 * Minute-scale p50 is generator queueing or broker overload, not a silent latency sample.
 * Schedule lag is reported beside service time; it does not replace it.
 * Saturation uses attempted sends (sent + skipped while offline). Clients that were
 * offline skipped their slot on purpose; that is CLIENTS_OFFLINE, not a short generator.
 */
export function assessLoad(input: {
  readonly offeredPerSec: number;
  readonly achievedSendPerSec: number;
  readonly achievedDeliveryPerSec: number;
  readonly scheduleLagP50Ms: number;
  readonly serviceP50Ms: number;
  /** sent + skippedWhileOffline, per second. Defaults to achievedSendPerSec. */
  readonly attemptedPerSec?: number;
}): LoadAssessment {
  const attemptedPerSec = input.attemptedPerSec ?? input.achievedSendPerSec;
  const offlineFraction = attemptedPerSec > 0
    ? Math.max(0, attemptedPerSec - input.achievedSendPerSec) / attemptedPerSec
    : 0;
  const reasons: string[] = [];
  if (input.scheduleLagP50Ms > 250) {
    reasons.push(`generator schedule lag p50 ${Math.round(input.scheduleLagP50Ms)}ms`);
  }
  if (input.offeredPerSec > 0 && attemptedPerSec < 0.95 * input.offeredPerSec) {
    reasons.push(
      `attempted send ${attemptedPerSec.toFixed(1)}/s is under 95% of offered ${input.offeredPerSec.toFixed(1)}/s`,
    );
  }
  if (input.serviceP50Ms > 5_000) {
    reasons.push(`service p50 ${Math.round(input.serviceP50Ms)}ms`);
  }
  return {
    offeredPerSec: input.offeredPerSec,
    achievedSendPerSec: input.achievedSendPerSec,
    attemptedPerSec,
    achievedDeliveryPerSec: input.achievedDeliveryPerSec,
    scheduleLagP50Ms: input.scheduleLagP50Ms,
    serviceP50Ms: input.serviceP50Ms,
    offlineFraction,
    clientsOffline: offlineFraction > 0.05,
    saturated: reasons.length > 0,
    saturationReason: reasons.length > 0 ? reasons.join('; ') : null,
  };
}
