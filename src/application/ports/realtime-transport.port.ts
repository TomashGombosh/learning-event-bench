import { StoredEvent } from '@domain/collab-event';

/** Pushes events to the clients connected to *this* gateway instance. */
export interface RealtimeTransportPort {
  emitToRoom(roomId: string, stored: StoredEvent): void;
}

export const REALTIME_TRANSPORT = Symbol('RealtimeTransportPort');
