/** End-to-end latency decomposition T = T_client + T_net + T_gw + T_broker + T_fan (ms). */
export interface LatencySample {
  readonly roomId: string;
  readonly totalMs: number;
  /** Scheduled -> gateway accepted (client + uplink network + gateway queue). */
  readonly uplinkMs?: number;
  /** Gateway accepted -> delivered to receiver (broker + fan-out + downlink). */
  readonly downlinkMs?: number;
}

export function latencyOf(
  roomId: string,
  scheduledAtMs: number,
  receivedAtMs: number,
  gatewayInAtMs?: number,
): LatencySample {
  const totalMs = receivedAtMs - scheduledAtMs;
  if (gatewayInAtMs === undefined) return { roomId, totalMs };
  return {
    roomId,
    totalMs,
    uplinkMs: gatewayInAtMs - scheduledAtMs,
    downlinkMs: receivedAtMs - gatewayInAtMs,
  };
}
