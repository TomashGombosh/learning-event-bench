import { CollabEvent, StoredEvent } from '@domain/collab-event';
import { EventCategory } from '@domain/event-category';
import { EventBusPort } from '@application/ports/event-bus.port';
import { ReplayableEventLogPort } from '@application/ports/replayable-event-log.port';

export interface BusUnderTest {
  readonly bus: EventBusPort;
  readonly log: ReplayableEventLogPort | null;
}

let roomCounter = 0;
const freshRoom = (): string => `contract-${Date.now()}-${++roomCounter}`;

const eventFor = (roomId: string, seq: number, clientId = 'a'): CollabEvent => ({
  roomId, clientId, seq, category: EventCategory.CollaborativeEditing, payload: `p${seq}`, scheduledAtMs: Date.now(),
});

async function waitFor(predicate: () => boolean, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timeout waiting for condition');
    await new Promise((r) => setTimeout(r, 10));
  }
}

/**
 * Liskov contract every EventBusPort adapter must satisfy.
 * Replay assertions run only for adapters that also implement ReplayableEventLogPort.
 */
export function runEventBusContract(name: string, create: () => Promise<BusUnderTest>): void {
  describe(`EventBusPort contract: ${name}`, () => {
    let subject: BusUnderTest;

    beforeAll(async () => {
      subject = await create();
    });
    afterAll(async () => subject.bus.close());

    it('delivers events of a room to its subscribers in publication order', async () => {
      const room = freshRoom();
      const received: StoredEvent[] = [];
      const sub = await subject.bus.subscribe(room, (s) => received.push(s));
      for (let i = 1; i <= 50; i++) await subject.bus.publish(eventFor(room, i));
      await waitFor(() => received.length === 50);
      expect(received.map((s) => s.event.seq)).toEqual(Array.from({ length: 50 }, (_, i) => i + 1));
      await sub.unsubscribe();
    });

    it('isolates rooms from each other', async () => {
      const roomA = freshRoom();
      const roomB = freshRoom();
      const receivedA: StoredEvent[] = [];
      const sub = await subject.bus.subscribe(roomA, (s) => receivedA.push(s));
      await subject.bus.publish(eventFor(roomB, 1));
      await subject.bus.publish(eventFor(roomA, 2));
      await waitFor(() => receivedA.length === 1);
      await new Promise((r) => setTimeout(r, 200));
      expect(receivedA.map((s) => s.event.roomId)).toEqual([roomA]);
      await sub.unsubscribe();
    });

    it('preserves the event payload and metadata', async () => {
      const room = freshRoom();
      const received: StoredEvent[] = [];
      const sub = await subject.bus.subscribe(room, (s) => received.push(s));
      const event = { ...eventFor(room, 7, 'xyz'), gatewayInAtMs: 123 };
      await subject.bus.publish(event);
      await waitFor(() => received.length === 1);
      expect(received[0].event).toEqual(event);
      await sub.unsubscribe();
    });

    it('replays exactly the events after the last seen id (if replay is supported)', async () => {
      if (subject.log === null) return;
      const room = freshRoom();
      const received: StoredEvent[] = [];
      const sub = await subject.bus.subscribe(room, (s) => received.push(s));
      for (let i = 1; i <= 10; i++) await subject.bus.publish(eventFor(room, i));
      await waitFor(() => received.length === 10);
      const missed = await subject.log.readAfter(room, received[3].id);
      expect(missed.map((s) => s.event.seq)).toEqual([5, 6, 7, 8, 9, 10]);
      const all = await subject.log.readAfter(room, null);
      expect(all.map((s) => s.event.seq)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      await sub.unsubscribe();
    });
  });
}
