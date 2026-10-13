import type { Server } from 'socket.io';
import { StoredEvent } from '@domain/collab-event';
import { RealtimeTransportPort } from '@application/ports/realtime-transport.port';

export const EVENT_MESSAGE = 'evt';

export class SocketIoTransport implements RealtimeTransportPort {
  private server: Server | null = null;

  attach(server: Server): void {
    this.server = server;
  }

  emitToRoom(roomId: string, stored: StoredEvent): void {
    this.server?.to(roomId).emit(EVENT_MESSAGE, stored);
  }
}
