import { InMemoryEventBus } from '@adapters/in-memory/in-memory-event-bus';
import { CATCH_UP_PAGE_SIZE, CatchUpAfterReconnectUseCase } from '@application/use-cases/catch-up.use-case';
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
    expect(result.supported && result.truncated).toBe(false);
  });

  it('CatchUp pages a long log instead of returning one unbounded ack', async () => {
    const bus = new InMemoryEventBus();
    const total = CATCH_UP_PAGE_SIZE + 3;
    for (let i = 1; i <= total; i++) await bus.publish(event(i));
    const first = await new CatchUpAfterReconnectUseCase(bus).execute('room', null);
    expect(first.supported && first.truncated).toBe(true);
    expect(first.supported && first.missed).toHaveLength(CATCH_UP_PAGE_SIZE);
    const lastId = first.supported ? first.missed[first.missed.length - 1].id : '';
    const second = await new CatchUpAfterReconnectUseCase(bus).execute('room', lastId);
    expect(second.supported && second.truncated).toBe(false);
    expect(second.supported && second.missed.map((s) => s.event.seq)).toEqual([
      CATCH_UP_PAGE_SIZE + 1, CATCH_UP_PAGE_SIZE + 2, CATCH_UP_PAGE_SIZE + 3,
    ]);
  });
});
