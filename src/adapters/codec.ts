import { CollabEvent } from '@domain/collab-event';

export function encodeEvent(event: CollabEvent): string {
  return JSON.stringify(event);
}

export function decodeEvent(raw: string): CollabEvent {
  return JSON.parse(raw) as CollabEvent;
}

export const roomStreamKey = (roomId: string): string => `room:${roomId}:events`;
