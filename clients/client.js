/**
 * Client library for talking to Kurir broker over TCP or HTTP/HTTPS.
 *
 * Supports:
 *   - Local TCP connection:   connect({ host: '127.0.0.1', port: 4000 })
 *   - Remote HTTP/S endpoint: connect({ url: 'https://brokerkurir.aselaboratory.com' })
 */

const net = require('node:net');
const { EventEmitter } = require('node:events');
const { attachJsonLines, send } = require('../src/protocol');

const DEFAULT_HOST = process.env.BROKER_HOST || '127.0.0.1';
const DEFAULT_PORT = Number(process.env.BROKER_PORT) || 4000;
const DEFAULT_URL = process.env.BROKER_URL || null;

/**
 * @param {{ host?: string, port?: number, url?: string, name?: string, role?: 'producer' | 'consumer' }} opts
 * @returns {Promise<BrokerClient>}
 */
async function connect({ host = DEFAULT_HOST, port = DEFAULT_PORT, url = DEFAULT_URL, name, role = 'producer' } = {}) {
  const targetUrl = url || (String(host).startsWith('http://') || String(host).startsWith('https://') ? host : null);
  if (targetUrl) {
    return connectHttp({ url: targetUrl.replace(/\/+$/, ''), name, role });
  }
  return connectTcp({ host, port, name, role });
}

function connectTcp({ host, port, name, role }) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port });
    const client = /** @type {BrokerClient} */ (new EventEmitter());

    client.publish = (routingKey, payload) => send(socket, { type: 'publish', routingKey, payload });
    client.subscribe = (queue, bindingKeys = []) => send(socket, { type: 'subscribe', queue, bindingKeys });
    client.close = () => socket.end();

    attachJsonLines(socket, (err, msg) => {
      if (err) return;
      if (msg.type === 'welcome') {
        client.id = msg.clientId;
        client.name = msg.name;
        resolve(client);
      }
      // 'error' is special on EventEmitters (throws if unhandled), so rename it.
      client.emit(msg.type === 'error' ? 'brokerError' : msg.type, msg);
    });

    socket.on('connect', () => send(socket, { type: 'hello', name, role }));
    socket.on('error', (err) => reject(err)); // no-op once resolved
    socket.on('close', () => client.emit('close'));
  });
}

async function connectHttp({ url, name, role }) {
  const client = /** @type {BrokerClient} */ (new EventEmitter());
  const clientId = `http_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
  client.id = clientId;
  client.name = name || (role === 'producer' ? `producer-${clientId}` : `consumer-${clientId}`);

  const abortController = new AbortController();

  client.publish = async (routingKey, payload) => {
    try {
      const res = await fetch(`${url}/api/publish`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ routingKey, payload, publisherName: client.name }),
        signal: abortController.signal,
      });
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`HTTP ${res.status}: ${text}`);
      }
      const data = await res.json();
      client.emit('published', data);
      return data;
    } catch (err) {
      client.emit('brokerError', { error: err.message });
    }
  };

  client.subscribe = async (queue, bindingKeys = []) => {
    const keys = Array.isArray(bindingKeys) ? bindingKeys : [bindingKeys].filter(Boolean);
    const streamUrl = `${url}/api/consumer/stream?name=${encodeURIComponent(client.name)}&queue=${encodeURIComponent(queue)}&keys=${encodeURIComponent(keys.join(','))}`;

    try {
      const res = await fetch(streamUrl, { signal: abortController.signal });
      if (!res.ok) {
        throw new Error(`Broker stream returned HTTP ${res.status}`);
      }

      client.emit('subscribed', { queue, bindingKeys: keys });

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const parts = buffer.split('\n\n');
        buffer = parts.pop();

        for (const part of parts) {
          const lines = part.split('\n');
          let eventType = 'message';
          let eventData = '';

          for (const line of lines) {
            if (line.startsWith('event: ')) eventType = line.slice(7).trim();
            if (line.startsWith('data: ')) eventData += line.slice(6);
          }

          if (eventType === 'message' && eventData) {
            try {
              const parsed = JSON.parse(eventData);
              client.emit('message', {
                queue: parsed.queue || queue,
                message: parsed.message,
              });
            } catch {}
          }
        }
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        client.emit('brokerError', { error: err.message });
      }
    } finally {
      client.emit('close');
    }
  };

  client.close = () => {
    abortController.abort();
    client.emit('close');
  };

  return client;
}

/**
 * @typedef {EventEmitter & {
 *   id: string, name: string,
 *   publish: (routingKey: string, payload?: any) => Promise<any> | void,
 *   subscribe: (queue: string, bindingKeys?: string[]) => Promise<any> | void,
 *   close: () => void,
 * }} BrokerClient
 */

module.exports = { connect };
