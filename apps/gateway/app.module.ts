import { DynamicModule, Module } from '@nestjs/common';
import { CLOCK, systemClock } from '@application/ports/clock.port';
import { PublishEventUseCase } from '@application/use-cases/publish-event.use-case';
import { RelayRoomEventsUseCase } from '@application/use-cases/relay-room-events.use-case';
import { CatchUpAfterReconnectUseCase } from '@application/use-cases/catch-up.use-case';
import { BrokerBundle } from '@adapters/broker-factory';
import { SocketIoTransport } from '@adapters/socketio/socketio-transport';
import { CollabGateway } from './collab.gateway';

/** Composition root: wires concrete adapters into use cases (Dependency Inversion). */
@Module({})
export class AppModule {
  static register(broker: BrokerBundle): DynamicModule {
    const transport = new SocketIoTransport();
    return {
      module: AppModule,
      providers: [
        CollabGateway,
        { provide: SocketIoTransport, useValue: transport },
        { provide: CLOCK, useValue: systemClock },
        { provide: PublishEventUseCase, useValue: new PublishEventUseCase(broker.bus, systemClock) },
        { provide: RelayRoomEventsUseCase, useValue: new RelayRoomEventsUseCase(broker.bus, transport) },
        { provide: CatchUpAfterReconnectUseCase, useValue: new CatchUpAfterReconnectUseCase(broker.log) },
      ],
    };
  }
}
