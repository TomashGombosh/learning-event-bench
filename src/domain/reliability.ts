/** Final replica state digest reported by one client at the end of a session. */
export interface ReplicaDigest {
  readonly roomId: string;
  readonly clientId: string;
  readonly digest: string;
}

export interface RoomOutcome {
  readonly roomId: string;
  readonly replicas: number;
  readonly diverged: boolean;
  /** Clients offline at the end of the cell. */
  readonly lostClients: number;
  /** Lost clients plus online clients whose digest is not the room majority. */
  readonly notConvergedClients: number;
}

/**
 * A room diverges when its online replicas do not share one digest, or when any
 * client is lost. A lost client (offline at the end: connect failed, or the
 * reconnect never reached a live gateway) makes the room diverged even if the
 * other digests match, so the room is not counted as converged while a member
 * never confirmed the final state.
 *
 * `notConvergedClients` counts lost clients and every online client whose digest
 * is not the strict majority. With no strict majority, every client in the room
 * is not converged. R = 1 - N_diverged / N_total.
 */
export function evaluateRooms(
  digests: readonly ReplicaDigest[],
  lostClientIds: ReadonlySet<string> = new Set(),
): RoomOutcome[] {
  const byRoom = new Map<string, ReplicaDigest[]>();
  for (const digest of digests) {
    const list = byRoom.get(digest.roomId) ?? [];
    list.push(digest);
    byRoom.set(digest.roomId, list);
  }
  return [...byRoom.entries()].map(([roomId, replicas]) => {
    const lost = replicas.filter((replica) => lostClientIds.has(replica.clientId));
    const online = replicas.filter((replica) => !lostClientIds.has(replica.clientId));
    const majority = majorityDigest(online);
    const onlineNotConverged = majority === null
      ? online.length
      : online.filter((replica) => replica.digest !== majority).length;
    const notConvergedClients = lost.length + onlineNotConverged;
    return {
      roomId,
      replicas: replicas.length,
      diverged: notConvergedClients > 0,
      lostClients: lost.length,
      notConvergedClients,
    };
  });
}

/** Strict majority digest, or null when there is a tie or nobody is online. */
function majorityDigest(online: readonly ReplicaDigest[]): string | null {
  const counts = new Map<string, number>();
  for (const replica of online) counts.set(replica.digest, (counts.get(replica.digest) ?? 0) + 1);
  let best: string | null = null;
  let bestCount = 0;
  let tied = false;
  for (const [digest, count] of counts) {
    if (count > bestCount) {
      best = digest;
      bestCount = count;
      tied = false;
    } else if (count === bestCount) tied = true;
  }
  if (tied || bestCount === 0) return null;
  return best;
}

/** R = 1 - N_diverged / N_total. */
export function reliability(outcomes: readonly RoomOutcome[]): number {
  if (outcomes.length === 0) throw new Error('No rooms to evaluate');
  const diverged = outcomes.filter((o) => o.diverged).length;
  return 1 - diverged / outcomes.length;
}
