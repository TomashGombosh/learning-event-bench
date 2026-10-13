import {
  ConnectedSocket, MessageBody, OnGatewayInit, SubscribeMessage, WebSocketGateway,
} from '@nestjs/websockets';
import { Inject } from '@nestjs/common';
import type { Server, Socket } from 'socket.io';
import { CollabEvent } from '@domain/collab-event';
import { PublishEventUseCase } from '@application/use-cases/publish-event.use-case';
import { RelayRoomEventsUseCase } from '@application/use-cases/relay-room-events.use-case';
import { CatchUpAfterReconnectUseCase, CatchUpResult } from '@application/use-cases/catch-up.use-case';
import { SocketIoTransport } from '@adapters/socketio/socketio-transport';

interface JoinRequest {
  readonly roomId: string;
  readonly lastSeenId: string | null;
}

/** Thin controller: translates Socket.IO messages into use-case calls. No business logic here. */
@WebSocketGateway({ cors: { origin: '*' } })
export class CollabGateway implements OnGatewayInit {
  constructor(
    @Inject(PublishEventUseCase) private readonly publishEvent: PublishEventUseCase,
    @Inject(RelayRoomEventsUseCase) private readonly relay: RelayRoomEventsUseCase,
    @Inject(CatchUpAfterReconnectUseCase) private readonly catchUp: CatchUpAfterReconnectUseCase,
    @Inject(SocketIoTransport) private readonly transport: SocketIoTransport,
  ) {}

  afterInit(server: Server): void {
    this.transport.attach(server);
  }

  @SubscribeMessage('join')
  async join(@ConnectedSocket() client: Socket, @MessageBody() request: JoinRequest): Promise<CatchUpResult> {
    await this.relay.ensureRelaying(request.roomId);
    await client.join(request.roomId);
    if (request.lastSeenId === undefined) return { supported: true, missed: [] };
    return this.catchUp.execute(request.roomId, request.lastSeenId);
  }

  @SubscribeMessage('op')
  async op(@MessageBody() event: CollabEvent): Promise<string> {
    return this.publishEvent.execute(event);
  }
}
