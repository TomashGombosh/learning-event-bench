import { CollabEvent, EventId, StoredEvent } from '@domain/collab-event';

export type EventHandler = (stored: StoredEvent) => void;

export interface Subscription {
  unsubscribe(): Promise<void>;
}

/** Publish/subscribe abstraction implemented by every broker adapter. */
export interface EventBusPort {
  readonly name: string;
  publish(event: CollabEvent): Promise<EventId>;
  subscribe(roomId: string, handler: EventHandler): Promise<Subscription>;
  close(): Promise<void>;
}

export const EVENT_BUS = Symbol('EventBusPort');
