import { EventCategory } from './event-category';

/** Broker-assigned position of an event in a room log (opaque to the domain). */
export type EventId = string;

/** A single operation produced by a client replica (e.g. a Yjs update). */
export interface CollabEvent {
  readonly roomId: string;
  readonly clientId: string;
  /** Per-client monotonically increasing sequence number. */
  readonly seq: number;
  readonly category: EventCategory;
  /** Base64-encoded CRDT update. */
  readonly payload: string;
  /** Time (ms, sender clock) at which the event was *scheduled* (avoids coordinated omission). */
  readonly scheduledAtMs: number;
  /** Time (ms, gateway clock) at which the gateway accepted the event. */
  readonly gatewayInAtMs?: number;
}

/** An event as stored/delivered by a broker, together with its log position. */
export interface StoredEvent {
  readonly id: EventId;
  readonly event: CollabEvent;
}

export function stampGatewayIn(event: CollabEvent, nowMs: number): CollabEvent {
  return { ...event, gatewayInAtMs: nowMs };
}
