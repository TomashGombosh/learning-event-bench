import { CollabEvent, EventId, stampGatewayIn } from '@domain/collab-event';
import { ClockPort } from '../ports/clock.port';
import { EventBusPort } from '../ports/event-bus.port';

/** Gateway accepted an operation from a client: stamp it and append it to the broker. */
export class PublishEventUseCase {
  constructor(
    private readonly bus: EventBusPort,
    private readonly clock: ClockPort,
  ) {}

  execute(event: CollabEvent): Promise<EventId> {
    return this.bus.publish(stampGatewayIn(event, this.clock.nowMs()));
  }
}
