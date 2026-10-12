/**
 * The broker core. Knows nothing about TCP or HTTP — it only manages
 * clients, queues, bindings and routing. The network layer (server.js)
 * calls into this class.
 *
 *   Producer ──publish(routingKey)──▶ [ Direct Exchange ] ──▶ Queue ──▶ Consumer(s)
 *                                       routing table:
 *                                       routingKey → queues
 *
 * Direct routing rules:
 *   • A queue is *bound* to one or more routing keys.
 *   • A message is copied into EVERY queue whose binding key equals the
 *     message's routing key exactly.
 *   • Within one queue, messages are handed to consumers round-robin
 *     (so several consumers on one queue share the work).
 *   • If a queue has no consumers, messages wait in the queue (buffering).
 *   • If no queue is bound to the key, the message is "unroutable" and dropped.
 */

const { EventEmitter } = require('node:events');
const { randomUUID } = require('node:crypto');

const shortId = () => randomUUID().slice(0, 8);

class Broker extends EventEmitter {
  constructor() {
    super();
    this.setMaxListeners(100); // one listener per open dashboard tab

    /** @type {Map<string, Queue>} queueName → queue */
    this.queues = new Map();
    /** @type {Map<string, Set<string>>} routingKey → set of queue names (the routing table) */
    this.bindings = new Map();
    /** @type {Map<string, Client>} clientId → client */
    this.clients = new Map();

    this.stats = { published: 0, routed: 0, delivered: 0, unroutable: 0 };
  }

  // ───────────────────────── Clients ─────────────────────────

  /**
   * @param {{ name?: string, role?: string, deliver: (message: object, queueName: string) => void }} opts
   */
  registerClient({ name, role = 'client', deliver }) {
    const id = shortId();
    const client = {
      id,
      name: String(name || `${role}-${id}`).slice(0, 40),
      role,
      deliver,
      queues: new Set(),
      connectedAt: Date.now(),
    };
    this.clients.set(id, client);
    this.#emit('client.connected', { client: publicClient(client) });
    return client;
  }

  removeClient(clientId) {
    const client = this.clients.get(clientId);
    if (!client) return;

    for (const queueName of client.queues) {
      const queue = this.queues.get(queueName);
      if (queue) queue.consumers = queue.consumers.filter((id) => id !== clientId);
    }
    this.clients.delete(clientId);
    this.#emit('client.disconnected', { clientId, name: client.name });
  }

  // ───────────────────────── Queues & bindings ─────────────────────────

  declareQueue(name) {
    assertName(name, 'Queue name');
    let queue = this.queues.get(name);
    if (!queue) {
      queue = { name, bindingKeys: new Set(), messages: [], consumers: [], nextConsumer: 0 };
      this.queues.set(name, queue);
      this.#emit('queue.declared', { queue: publicQueue(queue) });
    }
    return queue;
  }

  bindQueue(queueName, routingKey) {
    assertName(routingKey, 'Routing key');
    const queue = this.declareQueue(queueName);
    if (queue.bindingKeys.has(routingKey)) return;

    queue.bindingKeys.add(routingKey);
    if (!this.bindings.has(routingKey)) this.bindings.set(routingKey, new Set());
    this.bindings.get(routingKey).add(queueName);
    this.#emit('queue.bound', { queue: queueName, routingKey });
  }

  /** Attach a client as a consumer of a queue, binding any keys first. */
  subscribe(clientId, queueName, bindingKeys = []) {
    const client = this.#getClient(clientId);
    const queue = this.declareQueue(queueName);
    for (const key of bindingKeys) this.bindQueue(queueName, key);

    if (!queue.consumers.includes(clientId)) {
      queue.consumers.push(clientId);
      client.queues.add(queueName);
      this.#emit('consumer.subscribed', { clientId, queue: queueName });
    }

    // Hand over anything that was waiting. Deferred so the caller can
    // reply "subscribed" before the first message arrives.
    queueMicrotask(() => this.#drain(queue));
  }

  // ───────────────────────── Publishing & routing ─────────────────────────

  /**
   * Route a message by its routing key. Returns which queues received a copy.
   * Note: the broker never looks inside `payload` — routing uses only the key.
   */
  publish(clientId, routingKey, payload) {
    assertName(routingKey, 'Routing key');
    const client = this.#getClient(clientId);

    const message = {
      id: shortId(),
      routingKey,
      payload: payload ?? null,
      publishedBy: client.name,
      timestamp: Date.now(),
    };
    this.stats.published++;
    this.#emit('message.published', { clientId, message });

    // 1. Look up the routing table.
    const targets = [...(this.bindings.get(routingKey) ?? [])];

    if (targets.length === 0) {
      this.stats.unroutable++;
      this.#emit('message.unroutable', { messageId: message.id, routingKey });
      return { id: message.id, routedTo: [] };
    }

    // 2. Put a copy into every matching queue.
    for (const queueName of targets) {
      const queue = this.queues.get(queueName);
      queue.messages.push(message);
      this.stats.routed++;
      this.#emit('message.routed', {
        messageId: message.id,
        routingKey,
        queue: queueName,
        depth: queue.messages.length,
      });
    }

    // 3. Try to deliver what we can right now.
    for (const queueName of targets) this.#drain(this.queues.get(queueName));

    return { id: message.id, routedTo: targets };
  }

  /** Deliver waiting messages to the queue's consumers, round-robin. */
  #drain(queue) {
    while (queue.messages.length > 0 && queue.consumers.length > 0) {
      const index = queue.nextConsumer % queue.consumers.length;
      queue.nextConsumer = index + 1;
      const consumer = this.clients.get(queue.consumers[index]);
      const message = queue.messages.shift();

      consumer.deliver(message, queue.name);
      this.stats.delivered++;
      this.#emit('message.delivered', {
        messageId: message.id,
        routingKey: message.routingKey,
        queue: queue.name,
        clientId: consumer.id,
        depth: queue.messages.length,
      });
    }
  }

  // ───────────────────────── Introspection ─────────────────────────

  snapshot() {
    return {
      stats: { ...this.stats },
      clients: [...this.clients.values()].map(publicClient),
      queues: [...this.queues.values()].map(publicQueue),
      bindings: Object.fromEntries([...this.bindings].map(([key, set]) => [key, [...set]])),
    };
  }

  #getClient(clientId) {
    const client = this.clients.get(clientId);
    if (!client) throw new Error(`Unknown client ${clientId}`);
    return client;
  }

  /** Every state change is broadcast so the dashboard (and logs) can follow along. */
  #emit(type, data) {
    this.emit('event', { type, ts: Date.now(), stats: { ...this.stats }, ...data });
  }
}

// ───────────────────────── Helpers ─────────────────────────

function assertName(value, label) {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${label} must be a non-empty string`);
  if (value.length > 100) throw new Error(`${label} is too long (max 100 chars)`);
}

function publicClient(c) {
  return { id: c.id, name: c.name, role: c.role, queues: [...c.queues], connectedAt: c.connectedAt };
}

function publicQueue(q) {
  return {
    name: q.name,
    bindingKeys: [...q.bindingKeys],
    consumers: [...q.consumers],
    depth: q.messages.length,
    messages: q.messages.slice(0, 50).map((m) => ({ id: m.id, routingKey: m.routingKey })),
  };
}

/**
 * @typedef {{ id: string, name: string, role: string, queues: Set<string>, connectedAt: number,
 *             deliver: (message: object, queueName: string) => void }} Client
 * @typedef {{ name: string, bindingKeys: Set<string>, messages: object[], consumers: string[],
 *             nextConsumer: number }} Queue
 */

module.exports = { Broker };
