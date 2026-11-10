/** Load-generator process CPU (user + system) per 1000 submitted events. */
export function cpuPerThousand(
  usage: { readonly user: number; readonly system: number },
  events: number,
): { cpuSeconds: number; cpuSecondsPer1000Events: number } {
  const cpuSeconds = (usage.user + usage.system) / 1e6;
  const cpuSecondsPer1000Events = events > 0 ? (cpuSeconds * 1000) / events : 0;
  return { cpuSeconds, cpuSecondsPer1000Events };
}

export interface ReplicaStateStats {
  readonly textBytes: number;
  readonly stateBytes: number;
  readonly metadataBytes: number;
}

export function sumReplicaStats(stats: readonly ReplicaStateStats[]): ReplicaStateStats {
  return stats.reduce((sum, item) => ({
    textBytes: sum.textBytes + item.textBytes,
    stateBytes: sum.stateBytes + item.stateBytes,
    metadataBytes: sum.metadataBytes + item.metadataBytes,
  }), { textBytes: 0, stateBytes: 0, metadataBytes: 0 });
}

/** Largest op-log metadata among the gateways that are still up. */
export async function serverMetadataBytes(urls: readonly string[]): Promise<number> {
  let best = 0;
  await Promise.all(urls.map(async (base) => {
    try {
      const response = await fetch(new URL('/bench/state', base));
      if (!response.ok) return;
      const body = await response.json() as { metadataBytes?: number | null };
      if (typeof body.metadataBytes === 'number') best = Math.max(best, body.metadataBytes);
    } catch {
      // The killed gateway is down. The survivors still answer.
    }
  }));
  return best;
}
