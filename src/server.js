/**
 * Network layer. Two servers share one Broker instance:
 *
 *   • TCP  :4000  → producers & consumers speak NDJSON (see protocol.js)
 *   • HTTP :8080  → web dashboard, live event stream (SSE), publish API
 *
 * Client → broker commands:
 *   {"type":"hello", "role":"producer"|"consumer", "name":"web-shop"}
 *   {"type":"publish", "routingKey":"order.created", "payload":{...}}
 *   {"type":"subscribe", "queue":"billing", "bindingKeys":["order.created"]}
 *
 * Broker → client replies:
 *   {"type":"welcome", "clientId":"..."}
 *   {"type":"published", "id":"...", "routedTo":["billing", ...]}
 *   {"type":"subscribed", "queue":"billing"}
 *   {"type":"message", "queue":"billing", "message":{id, routingKey, payload, publishedBy, timestamp}}
 *   {"type":"error", "error":"..."}
 */

const net = require('node:net');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { Broker } = require('./broker');
const { attachJsonLines, send } = require('./protocol');

const TCP_PORT = Number(process.env.BROKER_PORT) || 4000;
const HTTP_PORT = Number(process.env.DASHBOARD_PORT) || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, '..', 'public');

const broker = new Broker();

// ═════════════════════════ TCP: producers & consumers ═════════════════════════

const tcpServer = net.createServer((socket) => {
  /** @type {ReturnType<Broker['registerClient']> | null} */
  let client = null;

  attachJsonLines(socket, (err, msg) => {
    if (err) return send(socket, { type: 'error', error: err.message });
    try {
      handleCommand(msg);
    } catch (e) {
      send(socket, { type: 'error', error: e.message, ref: msg?.ref });
    }
  });

  function handleCommand(msg) {
    if (msg.type === 'hello') {
      if (client) throw new Error('Already said hello');
      client = broker.registerClient({
        name: msg.name,
        role: msg.role === 'consumer' ? 'consumer' : 'producer',
        deliver: (message, queue) => send(socket, { type: 'message', queue, message }),
      });
      return send(socket, { type: 'welcome', clientId: client.id, name: client.name });
    }

    if (!client) throw new Error('Send {"type":"hello"} first');

    switch (msg.type) {
      case 'publish': {
        const result = broker.publish(client.id, msg.routingKey, msg.payload);
        return send(socket, { type: 'published', ref: msg.ref, ...result });
      }
      case 'subscribe': {
        const keys = Array.isArray(msg.bindingKeys) ? msg.bindingKeys : [];
        broker.subscribe(client.id, msg.queue, keys);
        return send(socket, { type: 'subscribed', queue: msg.queue, bindingKeys: keys });
      }
      default:
        throw new Error(`Unknown command "${msg.type}"`);
    }
  }

  socket.on('close', () => client && broker.removeClient(client.id));
  socket.on('error', () => {}); // 'close' always follows; nothing else to do
});

// ═════════════════════════ HTTP: dashboard ═════════════════════════

// The dashboard's "Publish" form acts as a producer too.
const dashboardClient = broker.registerClient({ name: 'dashboard', role: 'producer', deliver: () => {} });

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;

  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  const isGetOrHead = req.method === 'GET' || req.method === 'HEAD';

  if (isGetOrHead && pathname === '/events') return handleEvents(req, res);
  if (isGetOrHead && pathname === '/api/state') return sendJson(res, 200, broker.snapshot());
  if (isGetOrHead && pathname === '/api/deliveries') return handleDeliveries(req, res, url);
  if (isGetOrHead && pathname === '/api/consumer/stream') return handleConsumerStream(req, res, url);
  if (req.method === 'POST' && pathname === '/api/publish') return handlePublish(req, res);
  if (isGetOrHead) return serveStatic(pathname, res);

  sendJson(res, 405, { error: 'Method not allowed' });
});

function handleConsumerStream(req, res, url) {
  const name = url.searchParams.get('name') || `http-consumer-${Date.now().toString(36)}`;
  const queue = url.searchParams.get('queue');
  const keysStr = url.searchParams.get('keys') || '';
  const bindingKeys = keysStr.split(',').map((k) => k.trim()).filter(Boolean);

  if (!queue) {
    return sendJson(res, 400, { error: 'Query param "queue" is required' });
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });
  res.write('retry: 2000\n\n');

  const client = broker.registerClient({
    name,
    role: 'consumer',
    deliver: (message, q) => {
      writeSse(res, 'message', { queue: q, message });
    },
  });

  broker.subscribe(client.id, queue, bindingKeys);
  writeSse(res, 'subscribed', { clientId: client.id, queue, bindingKeys });

  const keepAlive = setInterval(() => res.write(': ping\n\n'), 15000);

  req.on('close', () => {
    clearInterval(keepAlive);
    broker.removeClient(client.id);
  });
}

function handleDeliveries(req, res, url) {
  const consumer = url.searchParams.get('consumer') || 'all';
  const queue = url.searchParams.get('queue') || 'all';
  const search = url.searchParams.get('search') || '';
  const limit = Math.min(200, Math.max(1, Number(url.searchParams.get('limit')) || 50));
  const offset = Math.max(0, Number(url.searchParams.get('offset')) || 0);

  const items = broker.getDeliveries({ consumer, queue, search, limit, offset });
  const knownConsumers = broker.getKnownConsumers();
  sendJson(res, 200, { items, knownConsumers });
}

/** Server-Sent Events: a one-way live stream of every broker event. */
function handleEvents(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write('retry: 2000\n\n');
  writeSse(res, 'snapshot', broker.snapshot());

  const onEvent = (event) => writeSse(res, 'broker', event);
  broker.on('event', onEvent);
  const keepAlive = setInterval(() => res.write(': ping\n\n'), 15000);

  req.on('close', () => {
    clearInterval(keepAlive);
    broker.off('event', onEvent);
  });
}

function writeSse(res, event, data) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function handlePublish(req, res) {
  let body = '';
  req.setEncoding('utf8');
  req.on('data', (chunk) => {
    body += chunk;
    if (body.length > 1_000_000) req.destroy();
  });
  req.on('end', () => {
    try {
      const { routingKey, payload, publisherName } = JSON.parse(body || '{}');
      let pub = dashboardClient;
      if (publisherName && publisherName !== 'dashboard') {
        pub = broker.registerClient({
          name: publisherName,
          role: 'producer',
          deliver: () => {},
        });
      }
      const result = broker.publish(pub.id, routingKey, payload);
      if (pub !== dashboardClient) {
        broker.removeClient(pub.id);
      }
      sendJson(res, 200, result);
    } catch (e) {
      sendJson(res, 400, { error: e.message });
    }
  });
}

function serveStatic(pathname, res) {
  let relative;
  try {
    relative = pathname === '/' ? 'index.html' : decodeURIComponent(pathname).slice(1);
  } catch {
    return sendJson(res, 400, { error: 'Bad path' });
  }

  const filePath = path.join(PUBLIC_DIR, relative);
  if (!filePath.startsWith(PUBLIC_DIR + path.sep)) return sendJson(res, 403, { error: 'Forbidden' });

  fs.readFile(filePath, (err, data) => {
    if (err) return sendJson(res, 404, { error: 'Not found' });
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(filePath)] ?? 'application/octet-stream',
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0',
    });
    res.end(data);
  });
}

function sendJson(res, status, obj) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  });
  res.end(JSON.stringify(obj));
}

// ═════════════════════════ Console log ═════════════════════════

const c = {
  dim: (s) => `\x1b[2m${s}\x1b[0m`,
  cyan: (s) => `\x1b[36m${s}\x1b[0m`,
  green: (s) => `\x1b[32m${s}\x1b[0m`,
  yellow: (s) => `\x1b[33m${s}\x1b[0m`,
  red: (s) => `\x1b[31m${s}\x1b[0m`,
  magenta: (s) => `\x1b[35m${s}\x1b[0m`,
};

const nameOf = (id) => broker.clients.get(id)?.name ?? id;

broker.on('event', (e) => {
  const time = c.dim(new Date(e.ts).toLocaleTimeString());
  const line = {
    'client.connected': () => `${c.green('+ connect')}    ${e.client.name} (${e.client.role})`,
    'client.disconnected': () => `${c.red('- disconnect')} ${e.name}`,
    'queue.declared': () => `${c.cyan('queue')}        ${e.queue.name}`,
    'queue.bound': () => `${c.cyan('bind')}         ${e.queue} ← ${c.magenta(e.routingKey)}`,
    'consumer.subscribed': () => `${c.cyan('subscribe')}    ${nameOf(e.clientId)} → ${e.queue}`,
    'message.published': () => `${c.yellow('publish')}      ${e.message.publishedBy} ▶ ${c.magenta(e.routingKey ?? e.message.routingKey)}`,
    'message.routed': () => `${c.dim('  route')}        → ${e.queue} ${c.dim(`(depth ${e.depth})`)}`,
    'message.unroutable': () => `${c.red('  unroutable')}   no queue bound to "${e.routingKey}", dropped`,
    'message.delivered': () => `${c.green('  deliver')}      ${e.queue} → ${nameOf(e.clientId)}`,
  }[e.type];
  if (line) console.log(`${time}  ${line()}`);
});

// ═════════════════════════ Start ═════════════════════════

function onListenError(label, port) {
  return (err) => {
    console.error(c.red(`✖ ${label} could not listen on port ${port}: ${err.message}`));
    process.exit(1);
  };
}

tcpServer.on('error', onListenError('Broker (TCP)', TCP_PORT));
httpServer.on('error', onListenError('Dashboard (HTTP)', HTTP_PORT));

tcpServer.listen(TCP_PORT, HOST, () => {
  httpServer.listen(HTTP_PORT, HOST, () => {
    console.log(`
  ${c.magenta('◆ Kurir')}: a tiny message broker

  Broker (TCP)   ${c.cyan(`tcp://localhost:${TCP_PORT}`)}
  Dashboard      ${c.cyan(`http://localhost:${HTTP_PORT}`)}

  Try:  npm run demo
        npm run consumer -- billing order.created
        npm run producer -- order.created "{\\"orderId\\": 1}"
`);
  });
});
