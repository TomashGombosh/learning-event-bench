/** Deterministic room -> partition mapping (FNV-1a), so replay knows where to look. */
export function partitionOf(roomId: string, partitions: number): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < roomId.length; i++) {
    hash ^= roomId.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash % partitions;
}

export const kafkaEventId = (partition: number, offset: string): string => `${partition}-${offset}`;

export function parseKafkaEventId(id: string): { partition: number; offset: bigint } {
  const [partition, offset] = id.split('-');
  return { partition: Number(partition), offset: BigInt(offset) };
}
