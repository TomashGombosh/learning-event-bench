import { InMemoryEventBus } from '@adapters/in-memory/in-memory-event-bus';
import { CatchUpAfterReconnectUseCase } from '@application/use-cases/catch-up.use-case';
import { PublishEventUseCase } from '@application/use-cases/publish-event.use-case';
import { RelayRoomEventsUseCase } from '@application/use-cases/relay-room-events.use-case';
import { StoredEvent } from '@domain/collab-event';
import { EventCategory } from '@domain/event-category';

const event = (seq: number) => ({
  roomId: 'room', clientId: 'a', seq, category: EventCategory.CollaborativeEditing, payload: '', scheduledAtMs: 1,
});

describe('use cases', () => {
  it('PublishEvent stamps the gateway time before publishing', async () => {
    const bus = new InMemoryEventBus();
    await new PublishEventUseCase(bus, { nowMs: () => 42 }).execute(event(1));
    const [stored] = await bus.readAfter('room', null);
    expect(stored.event.gatewayInAtMs).toBe(42);
  });

  it('RelayRoomEvents subscribes once per room and forwards to the transport', async () => {
    const bus = new InMemoryEventBus();
    const emitted: StoredEvent[] = [];
    const relay = new RelayRoomEventsUseCase(bus, { emitToRoom: (_r, s) => emitted.push(s) });
    await relay.ensureRelaying('room');
    await relay.ensureRelaying('room');
    await bus.publish(event(1));
    expect(emitted).toHaveLength(1);
    await relay.stopAll();
  });

  it('CatchUp reports unsupported for brokers without replay', async () => {
    expect(await new CatchUpAfterReconnectUseCase(null).execute('room', null)).toEqual({ supported: false });
  });

  it('CatchUp returns only missed events', async () => {
    const bus = new InMemoryEventBus();
    for (let i = 1; i <= 3; i++) await bus.publish(event(i));
    const result = await new CatchUpAfterReconnectUseCase(bus).execute('room', '1');
    expect(result.supported && result.missed.map((s) => s.event.seq)).toEqual([2, 3]);
  });
});
