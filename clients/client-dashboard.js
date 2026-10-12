#!/usr/bin/env node
/**
 * Local Interactive Client Dashboard Server.
 * Runs on http://localhost:3000 (no terminal typing required!).
 *
 * Features:
 *   1. Producer Studio: Preset buttons + custom message publisher
 *   2. Consumer Studio: Spawn and stop multiple live TCP consumers visually
 *   3. Live Inbox: See messages processed by your consumers in real-time
 *   4. Auto-Traffic Simulator: One-click background traffic generator
 */

const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');
const { connect } = require('./client');

const CLIENT_PORT = Number(process.env.CLIENT_PORT) || 3000;
const BROKER_HOST = process.env.BROKER_HOST || '127.0.0.1';
const BROKER_PORT = Number(process.env.BROKER_PORT) || 4000;
const UI_DIR = path.join(__dirname, 'ui');

// Ensure UI dir exists
if (!fs.existsSync(UI_DIR)) {
  fs.mkdirSync(UI_DIR, { recursive: true });
}

// ───────────────────────── State ─────────────────────────

let publisherClient = null;
const activeConsumers = new Map(); // consumerId -> { client, name, queue, keys, receivedCount, connectedAt }
const receivedMessages = []; // array of last 100 received messages
const sseClients = new Set();
let simulatorTimer = null;
let simulatorRunning = false;

// ───────────────────────── TCP Client Logic ─────────────────────────

async function getPublisher() {
  if (publisherClient) return publisherClient;
  publisherClient = await connect({
    host: BROKER_HOST,
    port: BROKER_PORT,
    name: 'web-client-publisher',
    role: 'producer',
  });
  publisherClient.on('close', () => {
    publisherClient = null;
    broadcast({ type: 'status', message: 'Publisher disconnected from broker' });
  });
  return publisherClient;
}

async function startConsumer({ name, queue, bindingKeys }) {
  const consumerId = `c_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
  const clientName = name || `consumer-${consumerId}`;

  const client = await connect({
    host: BROKER_HOST,
    port: BROKER_PORT,
    name: clientName,
    role: 'consumer',
  });

  const record = {
    id: consumerId,
    name: clientName,
    queue,
    bindingKeys: Array.isArray(bindingKeys) ? bindingKeys : [bindingKeys].filter(Boolean),
    receivedCount: 0,
    connectedAt: Date.now(),
    client,
  };

  client.subscribe(record.queue, record.bindingKeys);

  client.on('message', ({ queue: q, message }) => {
    record.receivedCount++;
    const item = {
      id: message.id,
      consumerId: record.id,
      consumerName: record.name,
      queue: q,
      routingKey: message.routingKey,
      payload: message.payload,
      publishedBy: message.publishedBy,
      timestamp: message.timestamp || Date.now(),
      receivedAt: Date.now(),
    };
    receivedMessages.unshift(item);
    if (receivedMessages.length > 200) receivedMessages.pop();

    broadcast({ type: 'message_received', item, consumer: publicConsumer(record) });
  });

  client.on('close', () => {
    activeConsumers.delete(consumerId);
    broadcast({ type: 'consumer_stopped', consumerId });
  });

  activeConsumers.set(consumerId, record);
  broadcast({ type: 'consumer_started', consumer: publicConsumer(record) });
  return record;
}

function stopConsumer(consumerId) {
  const record = activeConsumers.get(consumerId);
  if (record) {
    record.client.close();
    activeConsumers.delete(consumerId);
    broadcast({ type: 'consumer_stopped', consumerId });
    return true;
  }
  return false;
}

function publicConsumer(c) {
  return {
    id: c.id,
    name: c.name,
    queue: c.queue,
    bindingKeys: c.bindingKeys,
    receivedCount: c.receivedCount,
    connectedAt: c.connectedAt,
  };
}

// ───────────────────────── Auto Simulator ─────────────────────────

const SAMPLE_EVENTS = [
  { key: 'order.created', payload: () => ({ orderId: Math.floor(1000 + Math.random() * 9000), customer: 'Budi Santoso', total: 150000 + Math.floor(Math.random() * 50) * 10000, items: ['Mechanical Keyboard', 'Desk Mat'] }) },
  { key: 'order.paid', payload: () => ({ orderId: Math.floor(1000 + Math.random() * 9000), method: 'qris', bank: 'BCA', amount: 350000 }) },
  { key: 'user.registered', payload: () => ({ userId: Math.floor(100 + Math.random() * 900), name: 'Citra Lestari', email: 'citra@example.com', registeredAt: new Date().toISOString() }) },
  { key: 'order.refunded', payload: () => ({ orderId: Math.floor(1000 + Math.random() * 9000), reason: 'Customer requested cancellation' }) },
];

function toggleSimulator() {
  if (simulatorRunning) {
    clearInterval(simulatorTimer);
    simulatorTimer = null;
    simulatorRunning = false;
  } else {
    simulatorRunning = true;
    simulatorTimer = setInterval(async () => {
      try {
        const pub = await getPublisher();
        const event = SAMPLE_EVENTS[Math.floor(Math.random() * SAMPLE_EVENTS.length)];
        pub.publish(event.key, event.payload());
      } catch (err) {
        console.error('Simulator publish error:', err.message);
      }
    }, 1500);
  }
  broadcast({ type: 'simulator_state', running: simulatorRunning });
  return simulatorRunning;
}

// ───────────────────────── HTTP Server & SSE ─────────────────────────

function broadcast(data) {
  const payload = `data: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(payload);
    } catch {
      sseClients.delete(res);
    }
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;

  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // SSE Stream
  if (req.method === 'GET' && pathname === '/api/stream') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    res.write('retry: 2000\n\n');
    sseClients.add(res);

    // Initial state
    const snapshot = {
      type: 'initial_state',
      brokerConfig: { host: BROKER_HOST, port: BROKER_PORT },
      simulatorRunning,
      consumers: [...activeConsumers.values()].map(publicConsumer),
      messages: receivedMessages.slice(0, 50),
    };
    res.write(`data: ${JSON.stringify(snapshot)}\n\n`);

    req.on('close', () => sseClients.delete(res));
    return;
  }

  // Publish API
  if (req.method === 'POST' && pathname === '/api/publish') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', async () => {
      try {
        const { routingKey, payload } = JSON.parse(body || '{}');
        if (!routingKey) throw new Error('Routing key is required');
        const pub = await getPublisher();

        pub.once('published', (result) => {
          sendJson(res, 200, { success: true, ...result });
        });
        pub.publish(routingKey, payload);
      } catch (err) {
        sendJson(res, 400, { success: false, error: err.message });
      }
    });
    return;
  }

  // Start Consumer API
  if (req.method === 'POST' && pathname === '/api/consumer/start') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', async () => {
      try {
        const { name, queue, bindingKeys } = JSON.parse(body || '{}');
        if (!queue) throw new Error('Queue name is required');
        const consumer = await startConsumer({ name, queue, bindingKeys });
        sendJson(res, 200, { success: true, consumer: publicConsumer(consumer) });
      } catch (err) {
        sendJson(res, 400, { success: false, error: err.message });
      }
    });
    return;
  }

  // Stop Consumer API
  if (req.method === 'POST' && pathname === '/api/consumer/stop') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      try {
        const { consumerId } = JSON.parse(body || '{}');
        const stopped = stopConsumer(consumerId);
        sendJson(res, 200, { success: stopped });
      } catch (err) {
        sendJson(res, 400, { success: false, error: err.message });
      }
    });
    return;
  }

  // Toggle Simulator API
  if (req.method === 'POST' && pathname === '/api/simulator/toggle') {
    const running = toggleSimulator();
    return sendJson(res, 200, { running });
  }

  // Clear Messages API
  if (req.method === 'POST' && pathname === '/api/messages/clear') {
    receivedMessages.length = 0;
    broadcast({ type: 'messages_cleared' });
    return sendJson(res, 200, { success: true });
  }

  // Serve Static Client UI
  if (req.method === 'GET') {
    const filePath = pathname === '/' ? path.join(UI_DIR, 'index.html') : path.join(UI_DIR, pathname);
    if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
      const ext = path.extname(filePath);
      const mime = { '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript' }[ext] || 'text/plain';
      res.writeHead(200, { 'Content-Type': mime });
      return fs.createReadStream(filePath).pipe(res);
    }
  }

  sendJson(res, 404, { error: 'Not found' });
});

function sendJson(res, code, data) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

server.listen(CLIENT_PORT, () => {
  console.log(`
  🚀 Kurir Client Studio Running!

  Local Web App:  \x1b[36mhttp://localhost:${CLIENT_PORT}\x1b[0m
  Target Broker:  \x1b[35mtcp://${BROKER_HOST}:${BROKER_PORT}\x1b[0m

  Use the dashboard to publish messages, manage consumers, and view received data!
  Press Ctrl+C to stop.
  `);
});
