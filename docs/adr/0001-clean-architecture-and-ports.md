# ADR 0001: Clean Architecture with segregated replay port

**Context.** The study compares three brokers with different capabilities; the comparison must not be biased by
broker-specific code in the business logic.

**Decision.** Domain and use cases are framework-free. Brokers are adapters behind `EventBusPort`.
Replay is a separate `ReplayableEventLogPort` (Interface Segregation) because Redis Pub/Sub cannot implement it;
`CatchUpAfterReconnectUseCase` receives `null` for such brokers and reports "unsupported".
All adapters must pass the same contract suite (Liskov substitution).

**Consequences.** A new broker (NATS, RabbitMQ) is a new adapter plus one line in `broker-factory.ts`.
Load balancing is done client-side (round-robin over gateway URLs with fail-over) instead of Nginx, to avoid an
extra hop in the measured path and to make gateway-kill fail-over explicit.
