/**
 * Minimal client library for talking to the broker over TCP.
 *
 *   const client = await connect({ name: 'web-shop', role: 'producer' });
 *   client.publish('order.created', { orderId: 42 });
 *
 *   const worker = await connect({ name: 'billing-1', role: 'consumer' });
 *   worker.subscribe('billing', ['order.created']);
 *   worker.on('message', ({ queue, message }) => { ... });
 */

const net = require('node:net');
const { EventEmitter } = require('node:events');
const { attachJsonLines, send } = require('../src/protocol');

const DEFAULT_HOST = process.env.BROKER_HOST || '127.0.0.1';
const DEFAULT_PORT = Number(process.env.BROKER_PORT) || 4000;

/**
 * @param {{ host?: string, port?: number, name?: string, role?: 'producer' | 'consumer' }} opts
 * @returns {Promise<BrokerClient>}
 */
function connect({ host = DEFAULT_HOST, port = DEFAULT_PORT, name, role = 'producer' } = {}) {
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

/**
 * @typedef {EventEmitter & {
 *   id: string, name: string,
 *   publish: (routingKey: string, payload?: any) => void,
 *   subscribe: (queue: string, bindingKeys?: string[]) => void,
 *   close: () => void,
 * }} BrokerClient
 */

module.exports = { connect };
