import type { Server } from 'node:http';
import { WebSocket, WebSocketServer } from 'ws';
import { memoryStateStats, OtStateStats } from '@adapters/sharedb/state-stats';

const WebSocketJSONStream = require('@teamwork/websocket-json-stream');

export interface OtStateSource {
  _writeOpSync?: unknown;
  close?: (callback?: (err?: Error | null) => void) => void;
}

/** ShareDB websocket on the gateway HTTP server, plus the op-log size probe. */
export function attachOtServer(server: Server, backend: { listen(stream: unknown): void }): { close(): Promise<void> } {
  const wss = new WebSocketServer({ server, path: '/sharedb' });
  wss.on('connection', (ws) => {
    const stream = new WebSocketJSONStream(ws);
    // A client that just dropped (netem, gateway kill) must not throw out of
    // the pubsub fanout. The JSON stream reports a closed socket via the
    // writable callback, and an unhandled stream 'error' fails the process.
    const write = stream.write.bind(stream);
    stream.write = (chunk: unknown, encoding?: unknown, callback?: unknown) => {
      if (ws.readyState !== WebSocket.OPEN) {
        const done = typeof callback === 'function' ? callback : typeof encoding === 'function' ? encoding : null;
        if (done) done();
        return false;
      }
      return write(chunk, encoding, callback);
    };
    stream.on('error', () => undefined);
    backend.listen(stream);
  });
  return {
    close: () => new Promise((resolve) => {
      for (const client of wss.clients) client.close();
      wss.close(() => resolve());
    }),
  };
}

export function mountOtState(expressApp: { get(path: string, handler: (req: unknown, res: OtResponse) => void): void }, db: OtStateSource): void {
  expressApp.get('/bench/state', (_req, res) => {
    res.json(otState(db));
  });
}

export function otState(db: OtStateSource): OtStateStats {
  if (typeof db._writeOpSync === 'function') return memoryStateStats(db as never);
  return { db: 'postgres', docs: 0, textBytes: 0, stateBytes: 0, metadataBytes: null };
}

interface OtResponse {
  json(body: unknown): void;
}
