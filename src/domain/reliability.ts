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
}

/** A room diverged if at least two of its replicas ended in different states. */
export function evaluateRooms(digests: readonly ReplicaDigest[]): RoomOutcome[] {
  const byRoom = new Map<string, Set<string>>();
  const counts = new Map<string, number>();
  for (const d of digests) {
    if (!byRoom.has(d.roomId)) byRoom.set(d.roomId, new Set());
    byRoom.get(d.roomId)!.add(d.digest);
    counts.set(d.roomId, (counts.get(d.roomId) ?? 0) + 1);
  }
  return [...byRoom.entries()].map(([roomId, states]) => ({
    roomId,
    replicas: counts.get(roomId)!,
    diverged: states.size > 1,
  }));
}

/** R = 1 - N_diverged / N_total. */
export function reliability(outcomes: readonly RoomOutcome[]): number {
  if (outcomes.length === 0) throw new Error('No rooms to evaluate');
  const diverged = outcomes.filter((o) => o.diverged).length;
  return 1 - diverged / outcomes.length;
}
