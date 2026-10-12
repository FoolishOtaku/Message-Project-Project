'use strict';

/**
 * Dashboard client. It listens to the broker's Server-Sent Events stream and:
 *   1. keeps a local copy of the broker state (clients, queues, bindings)
 *   2. animates every message hop: producer → exchange → queue(s) → consumer
 *
 * The broker emits publish/route/deliver events within the same millisecond,
 * so each hop's animation is chained on a promise for the previous hop.
 */

// ═════════════════════════ DOM ═════════════════════════

const $ = (id) => document.getElementById(id);
const els = {
  stage: $('stage'),
  fx: $('fx-layer'),
  exchange: $('exchange'),
  deadLetter: $('dead-letter'),
  deadCount: $('dead-count'),
  producers: $('producers'),
  consumers: $('consumers'),
  queues: $('queues'),
  routingRows: $('routing-rows'),
  keySuggestions: $('key-suggestions'),
  log: $('event-log'),
  form: $('publish-form'),
  keyInput: $('routing-key-input'),
  payloadInput: $('payload-input'),
  publishButton: $('publish-button'),
  feedback: $('publish-feedback'),
  connection: $('connection-status'),
  connectionText: $('connection-text'),
  clearLog: $('clear-log-button'),
  stat: {
    published: $('stat-published'),
    routed: $('stat-routed'),
    delivered: $('stat-delivered'),
    unroutable: $('stat-unroutable'),
    waiting: $('stat-waiting'),
  },
  deliveriesGrid: $('deliveries-grid'),
  filterConsumer: $('filter-consumer'),
  filterQueue: $('filter-queue'),
  filterSearch: $('filter-search'),
  btnRefreshDeliveries: $('btn-refresh-deliveries'),
};

const ICON = {
  producer: '<svg viewBox="0 0 24 24"><path d="M4 12h13M12 6l6 6-6 6"/></svg>',
  consumer: '<svg viewBox="0 0 24 24"><path d="M3 13h5l2 3h4l2-3h5"/><path d="M5.5 5h13L21 13v6H3v-6z"/></svg>',
  queue: '<svg viewBox="0 0 24 24"><path d="M12 3l9 5-9 5-9-5 9-5z"/><path d="M3 13l9 5 9-5"/></svg>',
};

const MAX_VISIBLE_ITEMS = 14;
const MAX_FLYING_DOTS = 80;
const FLY_MS = 620;
const ROSE = 'hsl(350 90% 64%)';

// ═════════════════════════ State ═════════════════════════

const state = {
  clients: new Map(), // id → { id, name, role, queues[] }
  queues: new Map(), // name → { name, bindingKeys[], consumers[], items[{id, routingKey}] }
  bindings: new Map(), // routingKey → queueName[]
  stats: { published: 0, routed: 0, delivered: 0, unroutable: 0 },
  sent: new Map(), // clientId → count published (seen by this tab)
  received: new Map(), // clientId → count received (seen by this tab)
};

/** Animation chains: messageId → publish hop, `${messageId}:${queue}` → route hop. */
const pending = new Map();
/** Route hops whose message was already delivered (don't park it in the queue). */
const deliveredEarly = new Set();

// ═════════════════════════ Helpers ═════════════════════════

const esc = (s) =>
  String(s).replace(/[&<>"']/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[ch]);

/** Stable color per routing key, so you can follow a key across the whole page. */
function colorFor(key) {
  let h = 0;
  for (const ch of key) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  return `hsl(${h % 360} 85% 66%)`;
}

const keyHtml = (k) => `<span class="k" style="--key-color:${colorFor(k)}">${esc(k)}</span>`;
const shortPayload = (p) => {
  const s = p == null ? '' : typeof p === 'string' ? p : JSON.stringify(p);
  return s.length > 60 ? s.slice(0, 57) + '…' : s;
};
const byKey = (key) => els.stage.querySelector(`[data-key="${CSS.escape(key)}"]`);
const clientEl = (id) => byKey(`client:${id}`);
const queueEl = (name) => byKey(`queue:${name}`);
const clientName = (id) => state.clients.get(id)?.name ?? id;

/**
 * Keyed list rendering: reuses existing elements so in-flight animations and
 * entry effects aren't reset on every update.
 */
function reconcile(container, items, keyOf, build, emptyHtml = '') {
  const existing = new Map();
  for (const el of container.querySelectorAll(':scope > [data-key]')) existing.set(el.dataset.key, el);
  container.querySelector(':scope > [data-empty]')?.remove();

  let anchor = null;
  for (const item of items) {
    const key = keyOf(item);
    const { tag = 'article', className, html = '', attrs = {} } = build(item);
    let el = existing.get(key);
    if (el) {
      existing.delete(key);
    } else {
      el = document.createElement(tag);
      el.className = className;
      el.dataset.key = key;
    }
    for (const [name, value] of Object.entries(attrs)) el.setAttribute(name, value);
    if (el._html !== html) {
      el.innerHTML = html;
      el._html = html;
    }
    const expected = anchor ? anchor.nextSibling : container.firstChild;
    if (el !== expected) container.insertBefore(el, expected);
    anchor = el;
  }

  for (const el of existing.values()) el.remove();
  if (!items.length && emptyHtml) container.insertAdjacentHTML('beforeend', emptyHtml);
}

const emptyBox = (html) => `<div class="empty" data-empty>${html}</div>`;

// ═════════════════════════ Rendering ═════════════════════════

function renderAll() {
  renderClients();
  renderQueues();
  renderRoutingTable();
  renderKeySuggestions();
  renderStats(state.stats, false);
}

function renderClients() {
  const all = [...state.clients.values()];

  reconcile(
    els.producers,
    all.filter((c) => c.role !== 'consumer'),
    (c) => `client:${c.id}`,
    (c) => ({
      className: 'node node-producer',
      html: `
        <div class="node-icon">${ICON.producer}</div>
        <div class="node-body">
          <div class="node-name">${esc(c.name)}</div>
          <div class="node-meta">${c.name === 'dashboard' ? 'this page' : `producer · ${esc(c.id)}`}</div>
        </div>
        <div class="node-count" title="Messages published">${state.sent.get(c.id) ?? 0}</div>`,
    }),
    emptyBox('No producers yet.<br>Run <code>npm run producer</code>')
  );

  reconcile(
    els.consumers,
    all.filter((c) => c.role === 'consumer'),
    (c) => `client:${c.id}`,
    (c) => ({
      className: 'node node-consumer',
      html: `
        <div class="node-icon">${ICON.consumer}</div>
        <div class="node-body">
          <div class="node-name">${esc(c.name)}</div>
          <div class="node-meta">from <b>${c.queues.length ? c.queues.map(esc).join(', ') : '…'}</b></div>
        </div>
        <div class="node-count" title="Messages received">${state.received.get(c.id) ?? 0}</div>`,
    }),
    emptyBox('No consumers yet.<br>Run <code>npm run consumer -- billing order.created</code>')
  );
}

function renderQueues() {
  reconcile(
    els.queues,
    [...state.queues.values()],
    (q) => `queue:${q.name}`,
    (q) => {
      const names = q.consumers.map(clientName);
      const keys = q.bindingKeys.length
        ? q.bindingKeys.map((k) => `<span class="key-chip" style="--key-color:${colorFor(k)}">${esc(k)}</span>`).join('')
        : '<span class="queue-items-empty">no bindings</span>';
      const foot =
        names.length === 0
          ? '<div class="queue-foot warn">No consumers: messages wait here</div>'
          : names.length === 1
            ? `<div class="queue-foot">→ ${esc(names[0])}</div>`
            : `<div class="queue-foot">→ ${names.length} consumers · round-robin</div>`;
      return {
        className: 'queue',
        html: `
          <div class="queue-head">
            <span class="queue-name">${ICON.queue}<span>${esc(q.name)}</span></span>
            <span class="queue-depth" title="Messages waiting">0</span>
          </div>
          <div class="queue-keys">${keys}</div>
          <div class="queue-items"></div>
          ${foot}`,
      };
    },
    emptyBox('No queues yet. A consumer creates one when it subscribes.')
  );
  for (const name of state.queues.keys()) renderQueueItems(name);
}

/** The little colored blocks inside a queue = messages waiting for a consumer. */
function renderQueueItems(name) {
  const q = state.queues.get(name);
  const el = queueEl(name);
  if (!q || !el) return;

  const depth = el.querySelector('.queue-depth');
  depth.textContent = q.items.length;
  depth.classList.toggle('has-items', q.items.length > 0);

  const box = el.querySelector('.queue-items');
  reconcile(
    box,
    q.items.slice(0, MAX_VISIBLE_ITEMS),
    (m) => `msg:${m.id}`,
    (m) => ({
      tag: 'span',
      className: 'queue-item',
      attrs: { style: `--key-color:${colorFor(m.routingKey)}`, title: `${m.routingKey} · ${m.id}` },
    }),
    '<span class="queue-items-empty" data-empty>empty</span>'
  );
  box.querySelector('.queue-more')?.remove();
  const extra = q.items.length - MAX_VISIBLE_ITEMS;
  if (extra > 0) box.insertAdjacentHTML('beforeend', `<span class="queue-more">+${extra}</span>`);
}

function renderRoutingTable() {
  reconcile(
    els.routingRows,
    [...state.bindings.entries()],
    ([key]) => `route:${key}`,
    ([key, queues]) => ({
      tag: 'div',
      className: 'routing-row',
      attrs: { style: `--key-color:${colorFor(key)}` },
      html: `<span class="key" title="${esc(key)}">${esc(key)}</span><span class="targets">→ ${queues.map(esc).join(', ')}</span>`,
    }),
    '<div class="routing-empty" data-empty>No bindings yet</div>'
  );
}

function renderKeySuggestions() {
  const keys = [...state.bindings.keys()];
  els.keySuggestions.innerHTML =
    keys.map((k) => `<button type="button" class="chip" style="--key-color:${colorFor(k)}" data-routing-key="${esc(k)}">${esc(k)}</button>`).join('') +
    '<button type="button" class="chip chip-ghost" data-routing-key="no.such.key" title="No queue is bound to this key, so it will be unroutable">no.such.key</button>';
}

function renderStats(stats, animate = true) {
  state.stats = stats;
  const values = { ...stats, waiting: Math.max(0, stats.routed - stats.delivered) };
  for (const [name, el] of Object.entries(els.stat)) {
    const next = String(values[name] ?? 0);
    if (el.textContent !== next) {
      el.textContent = next;
      if (animate) restartClass(el, 'bump');
    }
  }
}

function bumpCount(map, clientId) {
  const n = (map.get(clientId) ?? 0) + 1;
  map.set(clientId, n);
  const counter = clientEl(clientId)?.querySelector('.node-count');
  if (counter) counter.textContent = n;
}

// ═════════════════════════ Animation ═════════════════════════

function restartClass(el, cls) {
  if (!el) return;
  el.classList.remove(cls);
  void el.offsetWidth; // force reflow so the animation restarts
  el.classList.add(cls);
}

function hit(el, color) {
  if (!el) return;
  if (color) el.style.setProperty('--hit-color', color);
  restartClass(el, 'hit');
}

function centerOf(el, stageRect) {
  const r = el.getBoundingClientRect();
  return { x: r.left - stageRect.left + r.width / 2, y: r.top - stageRect.top + r.height / 2 };
}

/** Fly a glowing dot from one element to another. Resolves when it lands. */
function fly(fromEl, toEl, color, dead = false) {
  if (!fromEl || !toEl || document.hidden || els.fx.childElementCount > MAX_FLYING_DOTS) {
    return Promise.resolve();
  }
  const stageRect = els.stage.getBoundingClientRect();
  const a = centerOf(fromEl, stageRect);
  const b = centerOf(toEl, stageRect);
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const lift = Math.min(70, Math.hypot(dx, dy) * 0.18);

  const dot = document.createElement('span');
  dot.className = dead ? 'dot dead' : 'dot';
  dot.style.setProperty('--c', color);
  els.fx.appendChild(dot);

  const anim = dot.animate(
    [
      { transform: `translate(${a.x}px, ${a.y}px) scale(0.3)`, opacity: 0 },
      { transform: `translate(${a.x + dx * 0.5}px, ${a.y + dy * 0.5 - lift}px) scale(1.1)`, opacity: 1, offset: 0.5 },
      { transform: `translate(${b.x}px, ${b.y}px) scale(0.5)`, opacity: 0.3 },
    ],
    { duration: FLY_MS, easing: 'cubic-bezier(.45,0,.25,1)' }
  );
  return anim.finished.catch(() => {}).then(() => dot.remove());
}

function track(key, promise) {
  pending.set(key, promise);
  promise.finally(() => {
    if (pending.get(key) === promise) pending.delete(key);
  });
}
const after = (key) => pending.get(key) ?? Promise.resolve();

function addItem(queueName, item) {
  const q = state.queues.get(queueName);
  if (!q || q.items.some((m) => m.id === item.id)) return;
  q.items.push(item);
  renderQueueItems(queueName);
}

function removeItem(queueName, messageId) {
  const q = state.queues.get(queueName);
  if (!q) return;
  const i = q.items.findIndex((m) => m.id === messageId);
  if (i !== -1) {
    q.items.splice(i, 1);
    renderQueueItems(queueName);
  }
}

// ═════════════════════════ Event log ═════════════════════════

function log(kind, label, html) {
  const li = document.createElement('li');
  li.innerHTML = `<time>${new Date().toLocaleTimeString([], { hour12: false })}</time><span class="tag tag-${kind}">${label}</span><span class="msg">${html}</span>`;
  els.log.prepend(li);
  while (els.log.children.length > 150) els.log.lastElementChild.remove();
}

// ═════════════════════════ Broker events ═════════════════════════

const toQueue = (q) => ({
  name: q.name,
  bindingKeys: [...q.bindingKeys],
  consumers: [...q.consumers],
  items: (q.messages ?? []).map((m) => ({ id: m.id, routingKey: m.routingKey })),
});

function applySnapshot(snap) {
  pending.clear();
  deliveredEarly.clear();
  els.fx.replaceChildren();
  state.clients = new Map(snap.clients.map((c) => [c.id, c]));
  state.queues = new Map(snap.queues.map((q) => [q.name, toQueue(q)]));
  state.bindings = new Map(Object.entries(snap.bindings));
  state.sent.clear();
  state.received.clear();
  els.deadCount.textContent = snap.stats.unroutable;
  state.stats = snap.stats;
  renderAll();
}

const handlers = {
  'client.connected'(e) {
    state.clients.set(e.client.id, e.client);
    renderClients();
    log('connect', 'join', `<b>${esc(e.client.name)}</b> connected as ${e.client.role}`);
  },

  'client.disconnected'(e) {
    state.clients.delete(e.clientId);
    for (const q of state.queues.values()) q.consumers = q.consumers.filter((id) => id !== e.clientId);
    renderClients();
    renderQueues();
    log('leave', 'left', `<b>${esc(e.name)}</b> disconnected`);
  },

  'queue.declared'(e) {
    if (!state.queues.has(e.queue.name)) state.queues.set(e.queue.name, toQueue(e.queue));
    renderQueues();
    log('info', 'queue', `queue <b>${esc(e.queue.name)}</b> declared`);
  },

  'queue.bound'(e) {
    const q = state.queues.get(e.queue);
    if (q && !q.bindingKeys.includes(e.routingKey)) q.bindingKeys.push(e.routingKey);
    const targets = state.bindings.get(e.routingKey) ?? [];
    if (!targets.includes(e.queue)) targets.push(e.queue);
    state.bindings.set(e.routingKey, targets);
    renderQueues();
    renderRoutingTable();
    renderKeySuggestions();
    log('info', 'bind', `<b>${esc(e.queue)}</b> ← ${keyHtml(e.routingKey)}`);
  },

  'consumer.subscribed'(e) {
    const q = state.queues.get(e.queue);
    if (q && !q.consumers.includes(e.clientId)) q.consumers.push(e.clientId);
    const c = state.clients.get(e.clientId);
    if (c && !c.queues.includes(e.queue)) c.queues.push(e.queue);
    renderClients();
    renderQueues();
    log('info', 'sub', `<b>${esc(clientName(e.clientId))}</b> consumes <b>${esc(e.queue)}</b>`);
  },

  // Hop 1: producer → exchange
  'message.published'(e) {
    const { message, clientId } = e;
    const color = colorFor(message.routingKey);
    bumpCount(state.sent, clientId);
    hit(clientEl(clientId), color);
    track(message.id, fly(clientEl(clientId), els.exchange, color).then(() => hit(els.exchange)));
    log('publish', 'pub', `<b>${esc(message.publishedBy)}</b> → ${keyHtml(message.routingKey)} <span class="payload">${esc(shortPayload(message.payload))}</span>`);
  },

  // Hop 2a: exchange → queue (one per matching binding)
  'message.routed'(e) {
    const color = colorFor(e.routingKey);
    const hop = `${e.messageId}:${e.queue}`;
    const p = after(e.messageId)
      .then(() => {
        restartClass(byKey(`route:${e.routingKey}`), 'flash');
        return fly(els.exchange, queueEl(e.queue), color);
      })
      .then(() => {
        hit(queueEl(e.queue), color);
        if (!deliveredEarly.has(hop)) addItem(e.queue, { id: e.messageId, routingKey: e.routingKey });
      });
    track(hop, p);
  },

  // Hop 2b: exchange → trash (no binding matched)
  'message.unroutable'(e) {
    after(e.messageId)
      .then(() => fly(els.exchange, els.deadLetter, ROSE, true))
      .then(() => {
        els.deadCount.textContent = state.stats.unroutable;
        hit(els.deadLetter);
      });
    log('drop', 'drop', `no queue bound to ${keyHtml(e.routingKey)}, so it was dropped`);
  },

  // Hop 3: queue → consumer
  'message.delivered'(e) {
    const color = colorFor(e.routingKey);
    const hop = `${e.messageId}:${e.queue}`;
    if (pending.has(hop)) deliveredEarly.add(hop);
    after(hop)
      .then(() => {
        deliveredEarly.delete(hop);
        removeItem(e.queue, e.messageId);
        return fly(queueEl(e.queue), clientEl(e.clientId), color);
      })
      .then(() => {
        bumpCount(state.received, e.clientId);
        hit(clientEl(e.clientId), color);
      });
    onLiveDelivery(e);
    log('deliver', 'recv', `<b>${esc(e.queue)}</b> → <b>${esc(clientName(e.clientId))}</b> ${keyHtml(e.routingKey)}`);
  },
};

function handleEvent(e) {
  if (e.stats) renderStats(e.stats);
  handlers[e.type]?.(e);
}

// ═════════════════════════ Live connection (SSE) ═════════════════════════

function setConnection(stateName, text) {
  els.connection.dataset.state = stateName;
  els.connectionText.textContent = text;
}

function connect() {
  const source = new EventSource('/events');
  source.addEventListener('open', () => setConnection('live', 'Live'));
  source.addEventListener('error', () => {
    if (source.readyState === EventSource.CLOSED) setConnection('down', 'Disconnected');
    else setConnection('down', 'Reconnecting…');
  });
  source.addEventListener('snapshot', (ev) => applySnapshot(JSON.parse(ev.data)));
  source.addEventListener('broker', (ev) => handleEvent(JSON.parse(ev.data)));
}

// ═════════════════════════ Publish form ═════════════════════════

function setFeedback(kind, text) {
  els.feedback.className = `feedback ${kind}`;
  els.feedback.textContent = text;
}

els.form.addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const routingKey = els.keyInput.value.trim();
  if (!routingKey) return;

  const raw = els.payloadInput.value.trim();
  let payload = raw || null;
  try {
    if (raw) payload = JSON.parse(raw);
  } catch {
    /* not JSON → send as plain text */
  }

  els.publishButton.disabled = true;
  try {
    const res = await fetch('/api/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ routingKey, payload }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Publish failed');
    if (data.routedTo.length) setFeedback('ok', `✔ Routed to ${data.routedTo.join(', ')}`);
    else setFeedback('warn', `⚠ No queue is bound to "${routingKey}", so the message was dropped`);
  } catch (err) {
    setFeedback('err', `✖ ${err.message}`);
  } finally {
    els.publishButton.disabled = false;
  }
});

els.keySuggestions.addEventListener('click', (ev) => {
  const chip = ev.target.closest('[data-routing-key]');
  if (!chip) return;
  els.keyInput.value = chip.dataset.routingKey;
  els.keyInput.focus();
});

els.clearLog.addEventListener('click', () => els.log.replaceChildren());

// ═════════════════════════ Consumer Deliveries History (SQLite) ═════════════════════════

let deliveriesData = [];
let knownConsumersList = [];

async function loadDeliveries() {
  const consumer = els.filterConsumer.value;
  const queue = els.filterQueue.value;
  const search = els.filterSearch.value.trim();

  const params = new URLSearchParams();
  if (consumer && consumer !== 'all') params.set('consumer', consumer);
  if (queue && queue !== 'all') params.set('queue', queue);
  if (search) params.set('search', search);

  try {
    const res = await fetch(`/api/deliveries?${params.toString()}`);
    if (!res.ok) throw new Error('Failed to fetch deliveries');
    const data = await res.json();
    deliveriesData = data.items || [];
    knownConsumersList = data.knownConsumers || [];
    updateConsumerDropdown();
    updateQueueDropdown();
    renderDeliveries();
  } catch (err) {
    console.error('Failed to load deliveries:', err);
    els.deliveriesGrid.innerHTML = `<div class="deliveries-empty">Could not load SQLite history: ${esc(err.message)}</div>`;
  }
}

function updateConsumerDropdown() {
  const current = els.filterConsumer.value;
  const options = ['<option value="all">All Consumers</option>'];
  for (const c of knownConsumersList) {
    options.push(`<option value="${esc(c)}"${c === current ? ' selected' : ''}>${esc(c)}</option>`);
  }
  els.filterConsumer.innerHTML = options.join('');
}

function updateQueueDropdown() {
  const current = els.filterQueue.value;
  const queueNames = [...state.queues.keys()];
  const options = ['<option value="all">All Queues</option>'];
  for (const q of queueNames) {
    options.push(`<option value="${esc(q)}"${q === current ? ' selected' : ''}>${esc(q)}</option>`);
  }
  els.filterQueue.innerHTML = options.join('');
}

function renderDeliveries() {
  if (!deliveriesData.length) {
    els.deliveriesGrid.innerHTML = '<div class="deliveries-empty">No consumer deliveries recorded matching the current filters.</div>';
    return;
  }

  els.deliveriesGrid.innerHTML = deliveriesData.map((item) => {
    const date = new Date(item.deliveredAt);
    const timeStr = `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
    const payloadStr = item.payload != null
      ? (typeof item.payload === 'object' ? JSON.stringify(item.payload, null, 2) : String(item.payload))
      : '(no payload)';

    return `
      <article class="delivery-card">
        <div class="delivery-card-top">
          <span class="delivery-consumer">👤 ${esc(item.consumerName)}</span>
          <time class="delivery-time" datetime="${date.toISOString()}">${timeStr}</time>
        </div>
        <div class="delivery-card-tags">
          <span class="key-chip" style="--key-color:${colorFor(item.routingKey)}">🏷️ ${esc(item.routingKey)}</span>
          <span class="key-chip" style="--key-color:var(--teal)">📦 queue: ${esc(item.queue)}</span>
        </div>
        <pre class="delivery-payload-box">${esc(payloadStr)}</pre>
        <div class="delivery-card-foot">
          <span>msg: <code>${esc(item.messageId)}</code></span>
          <span>pub by: <b>${esc(item.publishedBy || 'client')}</b></span>
        </div>
      </article>
    `;
  }).join('');
}

function onLiveDelivery(e) {
  const item = {
    messageId: e.messageId,
    routingKey: e.routingKey,
    queue: e.queue,
    consumerId: e.clientId,
    consumerName: e.consumerName || clientName(e.clientId),
    deliveredAt: Date.now(),
    payload: e.payload,
    publishedBy: e.publishedBy || '',
  };

  if (!knownConsumersList.includes(item.consumerName)) {
    knownConsumersList.push(item.consumerName);
    updateConsumerDropdown();
  }

  // Prepend to visible list if matches current filters
  const currentConsumer = els.filterConsumer.value;
  const currentQueue = els.filterQueue.value;
  const matchesConsumer = currentConsumer === 'all' || currentConsumer === item.consumerName;
  const matchesQueue = currentQueue === 'all' || currentQueue === item.queue;

  if (matchesConsumer && matchesQueue) {
    deliveriesData.unshift(item);
    if (deliveriesData.length > 100) deliveriesData.pop();
    renderDeliveries();
  }
}

// Filter listeners
els.filterConsumer.addEventListener('change', loadDeliveries);
els.filterQueue.addEventListener('change', loadDeliveries);

let searchDebounceTimer = null;
els.filterSearch.addEventListener('input', () => {
  clearTimeout(searchDebounceTimer);
  searchDebounceTimer = setTimeout(loadDeliveries, 300);
});

els.btnRefreshDeliveries.addEventListener('click', loadDeliveries);

renderAll();
connect();
loadDeliveries();
