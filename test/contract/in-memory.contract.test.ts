import { InMemoryEventBus } from '@adapters/in-memory/in-memory-event-bus';
import { runEventBusContract } from './event-bus.contract';

runEventBusContract('in-memory', async () => {
  const bus = new InMemoryEventBus();
  return { bus, log: bus };
});
