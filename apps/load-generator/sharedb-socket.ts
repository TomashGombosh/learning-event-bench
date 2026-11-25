import { WebSocket } from 'ws';

/** The socket shape `sharedb/lib/client` Connection expects (browser WebSocket). */
export interface ShareSocket {
  readyState: number;
  send(data: string): void;
  close(reason?: string): void;
  onmessage: ((event: { data: string }) => void) | null;
  onopen: (() => void) | null;
  onclose: ((reason?: string) => void) | null;
  onerror: ((error: Error) => void) | null;
}

export function sharedbUrl(httpUrl: string): string {
  const url = new URL(httpUrl);
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  url.pathname = '/sharedb';
  url.search = '';
  url.hash = '';
  return url.toString();
}

export function browserSocket(ws: WebSocket): ShareSocket {
  const socket: ShareSocket = {
    readyState: ws.readyState,
    send(data: string) {
      if (ws.readyState === WebSocket.OPEN) ws.send(data);
    },
    close() {
      if (ws.readyState !== WebSocket.CLOSED) ws.close();
    },
    onmessage: null,
    onopen: null,
    onclose: null,
    onerror: null,
  };
  ws.on('message', (data) => {
    if (socket.readyState !== WebSocket.OPEN) return;
    socket.onmessage?.({ data: data.toString() });
  });
  ws.on('open', () => {
    socket.readyState = ws.readyState;
    socket.onopen?.();
  });
  ws.on('close', () => {
    socket.readyState = WebSocket.CLOSED;
    // Not 'closed': ShareDB treats that as a permanent client close.
    socket.onclose?.('disconnected');
  });
  ws.on('error', (error) => socket.onerror?.(error));
  return socket;
}

export function waitUntilOpen(ws: WebSocket, timeoutMs: number): Promise<void> {
  if (ws.readyState === WebSocket.OPEN) return Promise.resolve();
  if (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED) {
    return Promise.reject(new Error('socket closed before open'));
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error('connect timeout'));
    }, timeoutMs);
    const cleanup = (): void => {
      clearTimeout(timer);
      ws.off('open', onOpen);
      ws.off('error', onError);
      ws.off('close', onClose);
    };
    const onOpen = (): void => {
      cleanup();
      resolve();
    };
    const onError = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const onClose = (): void => {
      cleanup();
      reject(new Error('socket closed before open'));
    };
    ws.once('open', onOpen);
    ws.once('error', onError);
    ws.once('close', onClose);
  });
}
