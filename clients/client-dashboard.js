#!/usr/bin/env node
/**
 * Local Interactive Client Dashboard Server.
 * Runs on http://localhost:3000.
 *
 * Can connect to:
 *   - Remote Deployed Broker via HTTPS: https://brokerkurir.aselaboratory.com
 *   - Local Broker via HTTP:            http://localhost:8080
 */

const http = require('node:http');
const path = require('node:path');
const fs = require('node:fs');

const CLIENT_PORT = Number(process.env.CLIENT_PORT) || 3000;
let brokerUrl = (process.env.BROKER_URL || 'https://brokerkurir.aselaboratory.com').replace(/\/+$/, '');
const UI_DIR = path.join(__dirname, 'ui');

if (!fs.existsSync(UI_DIR)) {
  fs.mkdirSync(UI_DIR, { recursive: true });
}

// ───────────────────────── State ─────────────────────────

const activeConsumers = new Map(); // consumerId -> { id, name, queue, bindingKeys, receivedCount, connectedAt, abortController }
const receivedMessages = []; // last 100 received messages
const sseClients = new Set();
let simulatorTimer = null;
let simulatorRunning = false;

// ───────────────────────── Remote Broker Operations ─────────────────────────

async function checkBrokerHealth(targetUrl = brokerUrl) {
  try {
    const res = await fetch(`${targetUrl}/api/state`, { signal: AbortSignal.timeout(4000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    let streamSupported = false;
    try {
      const pingAbort = new AbortController();
      const timeout = setTimeout(() => pingAbort.abort(), 2000);
      const streamRes = await fetch(`${targetUrl}/api/consumer/stream?name=__ping__&queue=__ping__&keys=ping`, {
        signal: pingAbort.signal,
      });
      clearTimeout(timeout);
      if (streamRes.status === 200) {
        streamSupported = true;
      }
      if (streamRes.body) streamRes.body.cancel().catch(() => {});
    } catch {
      // ignore stream check errors
    }

    return { ok: true, state: data, url: targetUrl, streamSupported };
  } catch (err) {
    return { ok: false, error: err.message, url: targetUrl, streamSupported: false };
  }
}

async function publishMessage({ routingKey, payload, publisherName = 'web-client' }) {
  const res = await fetch(`${brokerUrl}/api/publish`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ routingKey, payload, publisherName }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Publish failed (${res.status}): ${text}`);
  }
  return res.json();
}

async function startHttpConsumer({ name, queue, bindingKeys }) {
  const consumerId = `c_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
  const clientName = name || `consumer-${consumerId}`;
  const keys = Array.isArray(bindingKeys) ? bindingKeys : [bindingKeys].filter(Boolean);

  const abortController = new AbortController();
  const streamUrl = `${brokerUrl}/api/consumer/stream?name=${encodeURIComponent(clientName)}&queue=${encodeURIComponent(queue)}&keys=${encodeURIComponent(keys.join(','))}`;

  let res;
  try {
    res = await fetch(streamUrl, { signal: abortController.signal });
  } catch (err) {
    throw new Error(`Cannot connect to broker stream at ${brokerUrl}: ${err.message}`);
  }

  if (!res.ok) {
    if (res.status === 404) {
      throw new Error(
        `Broker at ${brokerUrl} returned HTTP 404 on consumer stream. The deployed container on NixOS has not been updated with the latest code yet. Please run 'git pull && docker compose up -d --build' on your NixOS server.`
      );
    }
    const text = await res.text().catch(() => '');
    throw new Error(`Broker stream returned HTTP ${res.status}: ${text}`);
  }

  const record = {
    id: consumerId,
    name: clientName,
    queue,
    bindingKeys: keys,
    receivedCount: 0,
    connectedAt: Date.now(),
    abortController,
  };

  activeConsumers.set(consumerId, record);
  broadcast({ type: 'consumer_started', consumer: publicConsumer(record) });

  // Read consumer stream loop asynchronously
  (async () => {
    try {
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
              record.receivedCount++;
              const item = {
                id: parsed.message?.id || `msg_${Date.now()}`,
                consumerId: record.id,
                consumerName: record.name,
                queue: parsed.queue || record.queue,
                routingKey: parsed.message?.routingKey || 'unknown',
                payload: parsed.message?.payload,
                publishedBy: parsed.message?.publishedBy || 'broker',
                timestamp: parsed.message?.timestamp || Date.now(),
                receivedAt: Date.now(),
              };
              receivedMessages.unshift(item);
              if (receivedMessages.length > 200) receivedMessages.pop();

              broadcast({ type: 'message_received', item, consumer: publicConsumer(record) });
            } catch (parseErr) {
              console.error('Error parsing consumer message payload:', parseErr);
            }
          }
        }
      }
    } catch (err) {
      if (err.name !== 'AbortError') {
        console.error(`Consumer ${record.name} stream error:`, err.message);
        broadcast({ type: 'consumer_error', consumerId, error: err.message });
      }
    } finally {
      activeConsumers.delete(consumerId);
      broadcast({ type: 'consumer_stopped', consumerId });
    }
  })();

  return record;
}

function stopConsumer(consumerId) {
  const record = activeConsumers.get(consumerId);
  if (record) {
    record.abortController.abort();
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

// ───────────────────────── Auto Traffic Simulator & Templates ─────────────────────────

const DEFAULT_TEMPLATES = [
  { name: 'billing-worker-1', queue: 'billing', bindingKeys: ['order.created', 'order.paid'] },
  { name: 'shipping-service', queue: 'shipping', bindingKeys: ['order.paid'] },
  { name: 'notification-worker', queue: 'notifications', bindingKeys: ['order.created', 'order.paid', 'user.registered'] },
  { name: 'inventory-audit', queue: 'inventory', bindingKeys: ['order.created', 'order.refunded'] },
];

const UNROUTABLE_EVENTS = [
  { key: 'nobody.listens', payload: () => ({ message: 'Testing unroutable routing key', timestamp: Date.now() }) },
  { key: 'analytics.orphan', payload: () => ({ event: 'orphan_click', path: '/deprecated-page', userId: 404 }) },
  { key: 'system.deadletter', payload: () => ({ alert: 'unbound_route_test', severity: 'warning' }) },
  { key: 'unbound.telemetry', payload: () => ({ metric: 'ambient_temp', value: 24.8, host: 'unmonitored-device' }) },
  { key: 'ghost.event', payload: () => ({ reason: 'No queue is bound to this key', generatedBy: 'adaptive-bot' }) },
];

const SAMPLE_CUSTOMERS = ['Budi Santoso', 'Citra Lestari', 'Agus Pratama', 'Dewi Anggraini', 'Eko Prasetyo', 'Siti Rahma'];
const SAMPLE_ITEMS = ['Mechanical Keyboard', 'Desk Mat', 'Wireless Mouse', 'USB-C Cable', 'Monitor Arm', 'Coffee Mug', 'Headphones'];

function generatePayloadFor(key) {
  const k = String(key || '').toLowerCase();

  if (k.includes('order.creat') || k.includes('order_creat')) {
    return {
      orderId: Math.floor(1000 + Math.random() * 9000),
      customer: SAMPLE_CUSTOMERS[Math.floor(Math.random() * SAMPLE_CUSTOMERS.length)],
      total: 150000 + Math.floor(Math.random() * 50) * 10000,
      items: [
        SAMPLE_ITEMS[Math.floor(Math.random() * SAMPLE_ITEMS.length)],
        SAMPLE_ITEMS[Math.floor(Math.random() * SAMPLE_ITEMS.length)],
      ],
      createdAt: new Date().toISOString(),
    };
  }

  if (k.includes('paid') || k.includes('payment')) {
    return {
      orderId: Math.floor(1000 + Math.random() * 9000),
      method: ['qris', 'bca', 'mandiri', 'gopay'][Math.floor(Math.random() * 4)],
      paid: true,
      amount: 250000 + Math.floor(Math.random() * 30) * 10000,
      paidAt: new Date().toISOString(),
    };
  }

  if (k.includes('user') || k.includes('register') || k.includes('auth')) {
    const name = SAMPLE_CUSTOMERS[Math.floor(Math.random() * SAMPLE_CUSTOMERS.length)];
    const email = name.toLowerCase().replace(/\s+/g, '.') + '@example.com';
    return {
      userId: Math.floor(100 + Math.random() * 900),
      name,
      email,
      registeredAt: new Date().toISOString(),
    };
  }

  if (k.includes('refund')) {
    return {
      orderId: Math.floor(1000 + Math.random() * 9000),
      reason: ['Customer cancellation', 'Defective product', 'Out of stock'][Math.floor(Math.random() * 3)],
      refundAmount: 150000 + Math.floor(Math.random() * 20) * 10000,
      processed: true,
    };
  }

  if (k.includes('ship') || k.includes('delivery')) {
    return {
      trackingNumber: 'TRK-' + Math.floor(100000 + Math.random() * 900000),
      courier: ['JNE', 'SiCepat', 'J&T', 'GoSend'][Math.floor(Math.random() * 4)],
      destination: ['Jakarta', 'Bandung', 'Surabaya', 'Medan'][Math.floor(Math.random() * 4)],
      status: 'in_transit',
    };
  }

  if (k.includes('inventory') || k.includes('stock')) {
    return {
      sku: 'SKU-' + Math.floor(100 + Math.random() * 900),
      stockDelta: -(Math.floor(Math.random() * 4) + 1),
      warehouse: 'WH-CENTRAL-1',
      updatedAt: new Date().toISOString(),
    };
  }

  if (k.includes('notif') || k.includes('alert') || k.includes('email')) {
    return {
      notificationId: 'notif_' + Math.floor(1000 + Math.random() * 9000),
      channel: ['email', 'push_notification', 'sms'][Math.floor(Math.random() * 3)],
      message: `Automatic alert notification for ${key}`,
      sentAt: new Date().toISOString(),
    };
  }

  // Dynamic fallback for any custom user routing key
  return {
    id: `evt_${Math.random().toString(36).slice(2, 8)}`,
    routingKey: key,
    metricValue: Math.floor(10 + Math.random() * 90),
    status: 'dispatched',
    generator: 'adaptive-traffic-bot',
    timestamp: new Date().toISOString(),
  };
}

let cachedRoutableKeys = [];
let lastKeyDiscovery = 0;

async function discoverActiveRoutingKeys() {
  const now = Date.now();
  if (now - lastKeyDiscovery < 2500 && cachedRoutableKeys.length > 0) {
    return cachedRoutableKeys;
  }

  const keys = new Set();

  // 1. Local active consumers in this client studio
  for (const c of activeConsumers.values()) {
    if (Array.isArray(c.bindingKeys)) {
      c.bindingKeys.forEach((k) => k && keys.add(k));
    }
  }

  // 2. Live routing table from target broker (/api/state)
  try {
    const res = await fetch(`${brokerUrl}/api/state`, { signal: AbortSignal.timeout(2000) });
    if (res.ok) {
      const state = await res.json();
      if (state.bindings) {
        Object.keys(state.bindings).forEach((k) => k && keys.add(k));
      }
      if (Array.isArray(state.queues)) {
        for (const q of state.queues) {
          if (Array.isArray(q.bindingKeys)) {
            q.bindingKeys.forEach((k) => k && keys.add(k));
          }
        }
      }
    }
  } catch {}

  // 3. Past deliveries logged in SQLite database (/api/deliveries)
  try {
    const res = await fetch(`${brokerUrl}/api/deliveries?limit=50`, { signal: AbortSignal.timeout(2000) });
    if (res.ok) {
      const data = await res.json();
      if (Array.isArray(data.items)) {
        for (const item of data.items) {
          if (item.routingKey) keys.add(item.routingKey);
        }
      }
    }
  } catch {}

  cachedRoutableKeys = [...keys];
  lastKeyDiscovery = now;
  return cachedRoutableKeys;
}

function toggleSimulator() {
  if (simulatorRunning) {
    clearInterval(simulatorTimer);
    simulatorTimer = null;
    simulatorRunning = false;
  } else {
    simulatorRunning = true;
    simulatorTimer = setInterval(async () => {
      try {
        const routableKeys = await discoverActiveRoutingKeys();

        // 20% randomized unroutable rate, OR if no consumers/keys exist yet:
        const shouldBeUnroutable = Math.random() < 0.20 || routableKeys.length === 0;

        if (shouldBeUnroutable) {
          const routableSet = new Set(routableKeys);
          const safeUnroutable = UNROUTABLE_EVENTS.filter((e) => !routableSet.has(e.key));
          const unroutable = safeUnroutable.length > 0
            ? safeUnroutable[Math.floor(Math.random() * safeUnroutable.length)]
            : { key: `unroutable.random.${Date.now().toString(36)}`, payload: () => ({ test: 'unroutable_random' }) };

          await publishMessage({
            routingKey: unroutable.key,
            payload: unroutable.payload(),
            publisherName: 'auto-traffic-bot',
          });
        } else {
          // Adapt to current consumers/database: pick from known routable keys!
          const key = routableKeys[Math.floor(Math.random() * routableKeys.length)];
          const payload = generatePayloadFor(key);

          await publishMessage({
            routingKey: key,
            payload,
            publisherName: 'auto-traffic-bot',
          });
        }
      } catch (err) {
        console.error('Simulator publish error:', err.message);
      }
    }, 1500);
  }
  broadcast({ type: 'simulator_state', running: simulatorRunning });
  return simulatorRunning;
}

// ───────────────────────── SSE Broadcast ─────────────────────────

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

// ───────────────────────── HTTP Server ─────────────────────────

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  // SSE Stream for Client Studio Frontend
  if (req.method === 'GET' && pathname === '/api/stream') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    res.write('retry: 2000\n\n');
    sseClients.add(res);

    const health = await checkBrokerHealth();
    const snapshot = {
      type: 'initial_state',
      brokerUrl,
      brokerHealth: health,
      simulatorRunning,
      consumers: [...activeConsumers.values()].map(publicConsumer),
      messages: receivedMessages.slice(0, 50),
    };
    res.write(`data: ${JSON.stringify(snapshot)}\n\n`);

    req.on('close', () => sseClients.delete(res));
    return;
  }

  // Set Target Broker URL
  if (req.method === 'POST' && pathname === '/api/config/broker') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', async () => {
      try {
        const { url: nextUrl } = JSON.parse(body || '{}');
        if (!nextUrl) throw new Error('Broker URL is required');
        brokerUrl = nextUrl.trim().replace(/\/+$/, '');
        const health = await checkBrokerHealth(brokerUrl);
        broadcast({ type: 'broker_config_changed', brokerUrl, health });
        sendJson(res, 200, { success: true, brokerUrl, health });
      } catch (err) {
        sendJson(res, 400, { success: false, error: err.message });
      }
    });
    return;
  }

  // Check Target Broker Health
  if (req.method === 'GET' && pathname === '/api/config/health') {
    const health = await checkBrokerHealth();
    return sendJson(res, 200, health);
  }

  // Get Deliveries for Consumers (from broker SQLite or local memory fallback)
  if (req.method === 'GET' && pathname === '/api/deliveries') {
    const consumer = url.searchParams.get('consumer') || 'all';
    try {
      const brokerRes = await fetch(`${brokerUrl}/api/deliveries?${url.searchParams.toString()}`, {
        signal: AbortSignal.timeout(3000),
      });
      if (brokerRes.ok) {
        const data = await brokerRes.json();
        return sendJson(res, 200, data);
      }
    } catch {}

    let items = receivedMessages;
    if (consumer && consumer !== 'all') {
      items = items.filter((m) => m.consumerName === consumer || m.consumerId === consumer);
    }
    return sendJson(res, 200, {
      items,
      knownConsumers: [...new Set(receivedMessages.map((m) => m.consumerName))],
    });
  }

  // Reset Broker Database API
  if (req.method === 'POST' && pathname === '/api/reset') {
    try {
      const brokerRes = await fetch(`${brokerUrl}/api/reset`, {
        method: 'POST',
        signal: AbortSignal.timeout(4000),
      });
      receivedMessages = [];
      broadcast({ type: 'reset' });
      const data = await brokerRes.json();
      return sendJson(res, brokerRes.status, data);
    } catch (err) {
      receivedMessages = [];
      broadcast({ type: 'reset' });
      return sendJson(res, 200, { success: true, localOnly: true, error: err.message });
    }
  }

  // Publish Message API
  if (req.method === 'POST' && pathname === '/api/publish') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', async () => {
      try {
        const { routingKey, payload, publisherName } = JSON.parse(body || '{}');
        if (!routingKey) throw new Error('Routing key is required');
        const result = await publishMessage({ routingKey, payload, publisherName });
        sendJson(res, 200, { success: true, ...result });
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
        const consumer = await startHttpConsumer({ name, queue, bindingKeys });
        sendJson(res, 200, { success: true, consumer: publicConsumer(consumer) });
      } catch (err) {
        sendJson(res, 400, { success: false, error: err.message });
      }
    });
    return;
  }

  // Spawn Default Template Consumers API
  if (req.method === 'POST' && pathname === '/api/consumer/spawn-templates') {
    (async () => {
      const started = [];
      const errors = [];

      for (const tpl of DEFAULT_TEMPLATES) {
        const alreadyActive = [...activeConsumers.values()].some(
          (c) => c.queue === tpl.queue && c.name === tpl.name
        );
        if (alreadyActive) continue;

        try {
          const consumer = await startHttpConsumer({
            name: tpl.name,
            queue: tpl.queue,
            bindingKeys: tpl.bindingKeys,
          });
          started.push(publicConsumer(consumer));
        } catch (err) {
          errors.push({ name: tpl.name, error: err.message });
        }
      }

      sendJson(res, 200, {
        success: true,
        startedCount: started.length,
        started,
        errors,
      });
    })().catch((err) => {
      sendJson(res, 500, { success: false, error: err.message });
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

  // Serve Client UI
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
  Target Broker:  \x1b[35m${brokerUrl}\x1b[0m

  Use the dashboard to publish messages, manage consumers, and view received data!
  Press Ctrl+C to stop.
  `);
});
