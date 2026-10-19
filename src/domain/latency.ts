/** End-to-end latency decomposition T = T_client + T_net + T_gw + T_broker + T_fan (ms). */
export interface LatencySample {
  readonly roomId: string;
  /** Scheduled -> received. Includes generator schedule lag (open-loop, not service time). */
  readonly totalMs: number;
  /** Scheduled -> gateway accepted (client + uplink network + gateway queue). */
  readonly uplinkMs?: number;
  /** Gateway accepted -> delivered to receiver (broker + fan-out + downlink). */
  readonly downlinkMs?: number;
  /** Actual send -> received, both on loadgen clocks. This is the service latency. */
  readonly serviceMs?: number;
  /** Scheduled -> actual send. Large values mean the generator event loop is behind. */
  readonly scheduleLagMs?: number;
  /** Sender's scheduled time. The reservoir uses it to show the sample covers the window. */
  readonly scheduledAtMs?: number;
}

export function latencyOf(
  roomId: string,
  scheduledAtMs: number,
  receivedAtMs: number,
  gatewayInAtMs?: number,
  sentAtMs?: number,
): LatencySample {
  const totalMs = receivedAtMs - scheduledAtMs;
  const sample: LatencySample = { roomId, totalMs, scheduledAtMs };
  const withPath = gatewayInAtMs === undefined ? sample : {
    ...sample,
    uplinkMs: gatewayInAtMs - scheduledAtMs,
    downlinkMs: receivedAtMs - gatewayInAtMs,
  };
  if (sentAtMs === undefined) return withPath;
  return { ...withPath, serviceMs: receivedAtMs - sentAtMs, scheduleLagMs: sentAtMs - scheduledAtMs };
}
