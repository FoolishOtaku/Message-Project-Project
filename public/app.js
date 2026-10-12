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
  modal: {
    backdrop: $('consumer-modal'),
    name: $('modal-consumer-name'),
    status: $('modal-consumer-status'),
    meta: $('modal-consumer-meta'),
    search: $('modal-search'),
    btnRefresh: $('modal-btn-refresh'),
    btnClose: $('modal-btn-close'),
    count: $('modal-count'),
    list: $('modal-messages-list'),
  },
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

  const liveConsumers = all.filter((c) => c.role === 'consumer');
  const pastConsumersEmpty = knownConsumersList.length > 0
    ? `<div class="empty">No live consumers connected.<br><span style="display:block;margin:6px 0 4px;font-size:0.75rem;color:var(--text-dim);font-weight:600;">PAST CONSUMERS IN DB (CLICK TO INSPECT):</span><div style="display:flex;flex-wrap:wrap;gap:6px;">${knownConsumersList.map(name => `<button type="button" class="node-click-pill" style="cursor:pointer;" onclick="openConsumerModal('${esc(name)}')">👤 ${esc(name)} ↗</button>`).join('')}</div></div>`
    : emptyBox('No consumers yet.<br>Run <code>npm run consumer -- billing order.created</code>');

  reconcile(
    els.consumers,
    liveConsumers,
    (c) => `client:${c.id}`,
    (c) => ({
      className: 'node node-consumer node-clickable',
      attrs: {
        'data-consumer-name': c.name,
        title: `Click to inspect messages routed to ${c.name}`,
      },
      html: `
        <div class="node-icon">${ICON.consumer}</div>
        <div class="node-body">
          <div class="node-name">${esc(c.name)}</div>
          <div class="node-meta">from <b>${c.queues.length ? c.queues.map(esc).join(', ') : '…'}</b></div>
        </div>
        <div class="node-right">
          <div class="node-count" title="Messages received">${state.received.get(c.id) ?? 0}</div>
          <button type="button" class="btn-inspect-pill">🔍 Inspect ↗</button>
        </div>`,
    }),
    pastConsumersEmpty
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
  els.deliveriesGrid.innerHTML = deliveriesData.map(renderMessageCard).join('');
}

// ═════════════════════════ Human-Readable Payload Formatter ═════════════════════════

function formatKeyLabel(key) {
  const specialMap = {
    orderid: 'Order ID',
    customerid: 'Customer ID',
    userid: 'User ID',
    productid: 'Product ID',
    id: 'ID',
    qris: 'QRIS',
    url: 'URL',
    api: 'API',
    registeredat: 'Registered At',
    createdat: 'Created At',
    updatedat: 'Updated At',
  };
  const lower = String(key).toLowerCase();
  if (specialMap[lower]) return specialMap[lower];

  return String(key)
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}

function formatValue(key, val) {
  if (val === null || val === undefined) {
    return '<span class="val-null" style="color:var(--text-faint);">—</span>';
  }

  const keyLower = String(key).toLowerCase();

  // Boolean
  if (typeof val === 'boolean') {
    return val
      ? '<span class="status-badge status-ok">✔ Yes (Confirmed)</span>'
      : '<span class="status-badge status-no">✖ No (False)</span>';
  }

  // Number
  if (typeof val === 'number') {
    if (keyLower.includes('total') || keyLower.includes('amount') || keyLower.includes('price') || keyLower.includes('fee')) {
      return `<span class="val-currency">💰 Rp ${val.toLocaleString()}</span>`;
    }
    if (keyLower.includes('id')) {
      return `<code class="val-id">#${val}</code>`;
    }
    return `<span class="val-number">${val.toLocaleString()}</span>`;
  }

  // Array
  if (Array.isArray(val)) {
    if (val.length === 0) return '<span style="color:var(--text-faint);">(empty list)</span>';
    if (val.every((x) => typeof x !== 'object')) {
      return `
        <div class="pill-list">
          ${val.map((item) => `<span class="pill-item">${esc(String(item))}</span>`).join('')}
        </div>
      `;
    }
    return renderHumanReadablePayload(val);
  }

  // Object
  if (typeof val === 'object') {
    return renderHumanReadablePayload(val);
  }

  // String
  const str = String(val);

  // Email
  if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(str)) {
    return `<a href="mailto:${esc(str)}" class="val-email">✉️ ${esc(str)}</a>`;
  }

  // ISO Date / Timestamp string
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(str)) {
    const d = new Date(str);
    if (!isNaN(d.getTime())) {
      return `<span class="val-date">📅 ${d.toLocaleDateString()} ${d.toLocaleTimeString()}</span>`;
    }
  }

  // Payment methods or specific status keywords
  if (keyLower.includes('method') || keyLower.includes('status')) {
    return `<span class="status-badge status-info">${esc(str.toUpperCase())}</span>`;
  }

  return `<span class="val-string">${esc(str)}</span>`;
}

function renderHumanReadablePayload(payload) {
  if (payload == null) {
    return '<div class="readable-empty">No payload data</div>';
  }

  let data = payload;
  if (typeof data === 'string') {
    try {
      data = JSON.parse(data);
    } catch {
      return `<div class="readable-text-box">💬 ${esc(data)}</div>`;
    }
  }

  if (typeof data !== 'object') {
    return `<div class="readable-text-box">${formatValue('Value', data)}</div>`;
  }

  if (Array.isArray(data)) {
    if (data.length === 0) return '<div class="readable-empty">Empty list [ ]</div>';
    return `
      <div style="display:flex; flex-direction:column; gap:8px;">
        ${data.map((item, idx) => `
          <div style="display:flex; align-items:flex-start; gap:8px;">
            <span style="font-family:var(--font-mono); font-size:0.75rem; color:var(--text-dim);">#${idx + 1}</span>
            <div style="flex:1;">${renderHumanReadablePayload(item)}</div>
          </div>
        `).join('')}
      </div>
    `;
  }

  const entries = Object.entries(data);
  if (entries.length === 0) {
    return '<div class="readable-empty">Empty object { }</div>';
  }

  const rowsHtml = entries.map(([k, v]) => {
    return `
      <div class="attr-row">
        <div class="attr-label">${esc(formatKeyLabel(k))}</div>
        <div class="attr-value">${formatValue(k, v)}</div>
      </div>
    `;
  }).join('');

  const rawJson = JSON.stringify(data, null, 2);

  return `
    <div class="readable-card-body">
      <div class="attr-table">
        ${rowsHtml}
      </div>
      <details class="raw-json-details">
        <summary class="raw-json-summary">🔍 View Raw JSON</summary>
        <pre class="raw-json-box"><code>${esc(rawJson)}</code></pre>
      </details>
    </div>
  `;
}

function formatRelativeTime(ts) {
  if (!ts) return '';
  const diffSec = Math.floor((Date.now() - Number(ts)) / 1000);
  if (diffSec < 5) return 'just now';
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  return `${Math.floor(diffHr / 24)}d ago`;
}

function renderMessageCard(item) {
  const date = new Date(item.deliveredAt || item.publishedAt || Date.now());
  const timeStr = `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`;
  const relTime = formatRelativeTime(item.deliveredAt || item.publishedAt);
  const bodyHtml = renderHumanReadablePayload(item.payload);

  return `
    <article class="readable-msg-card">
      <div class="readable-msg-head">
        <div class="readable-msg-tags">
          <span class="delivery-consumer" style="cursor:pointer;" onclick="openConsumerModal('${esc(item.consumerName)}')" title="Click to pop out consumer messages">
            👤 <strong>${esc(item.consumerName)}</strong> ↗
          </span>
          <span class="key-chip" style="--key-color:${colorFor(item.routingKey)}">🏷️ ${esc(item.routingKey)}</span>
          <span class="key-chip" style="--key-color:var(--teal)">📦 ${esc(item.queue)}</span>
        </div>
        <div class="readable-msg-meta">
          <time datetime="${date.toISOString()}" title="${date.toISOString()}">🕒 ${relTime} (${timeStr})</time>
          <span>msg: <code>#${esc(item.messageId || item.id)}</code></span>
          <span>pub by: <b>${esc(item.publishedBy || 'broker')}</b></span>
        </div>
      </div>
      ${bodyHtml}
    </article>
  `;
}

// ═════════════════════════ Consumer Popout Modal Logic ═════════════════════════

let activeModalConsumer = null;
let modalDeliveries = [];

async function openConsumerModal(consumerName) {
  if (!consumerName) return;
  activeModalConsumer = consumerName;

  els.modal.name.textContent = consumerName;
  els.modal.search.value = '';

  const liveClient = [...state.clients.values()].find((c) => c.name === consumerName);
  if (liveClient) {
    els.modal.status.textContent = 'Active (Connected)';
    els.modal.status.className = 'modal-status-badge';
    els.modal.meta.textContent = `Listening on queue: ${liveClient.queues.join(', ') || 'none'}`;
  } else {
    els.modal.status.textContent = 'Database History';
    els.modal.status.className = 'modal-status-badge offline';
    els.modal.meta.textContent = 'Stored persistently in SQLite database';
  }

  els.modal.backdrop.style.display = 'flex';
  els.modal.backdrop.setAttribute('aria-hidden', 'false');
  document.body.style.overflow = 'hidden';

  await reloadModalDeliveries();
}

async function reloadModalDeliveries() {
  if (!activeModalConsumer) return;
  els.modal.list.innerHTML = '<div class="deliveries-empty">Loading messages from SQLite database...</div>';

  try {
    const res = await fetch(`/api/deliveries?consumer=${encodeURIComponent(activeModalConsumer)}&limit=100`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    modalDeliveries = data.items || [];
    renderModalMessages();
  } catch (err) {
    els.modal.list.innerHTML = `<div class="deliveries-empty">Failed to load consumer messages: ${esc(err.message)}</div>`;
  }
}

function renderModalMessages() {
  const query = (els.modal.search.value || '').trim().toLowerCase();
  let filtered = modalDeliveries;

  if (query) {
    filtered = filtered.filter((m) => {
      const matchKey = (m.routingKey || '').toLowerCase().includes(query);
      const matchQueue = (m.queue || '').toLowerCase().includes(query);
      const matchId = String(m.messageId || m.id || '').toLowerCase().includes(query);
      const matchPayload = JSON.stringify(m.payload || '').toLowerCase().includes(query);
      return matchKey || matchQueue || matchId || matchPayload;
    });
  }

  els.modal.count.textContent = filtered.length;

  if (!filtered.length) {
    els.modal.list.innerHTML = `
      <div class="deliveries-empty">
        ${query ? `No messages found matching "${esc(query)}"` : `No messages have been routed to consumer "${esc(activeModalConsumer)}" yet.`}
      </div>
    `;
    return;
  }

  els.modal.list.innerHTML = filtered.map(renderMessageCard).join('');
}

function closeConsumerModal() {
  els.modal.backdrop.style.display = 'none';
  els.modal.backdrop.setAttribute('aria-hidden', 'true');
  document.body.style.overflow = '';
  activeModalConsumer = null;
}

// ═════════════════════════ Event Handling ═════════════════════════

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

  // If consumer modal is currently open for this consumer, update live!
  if (activeModalConsumer && activeModalConsumer === item.consumerName) {
    modalDeliveries.unshift(item);
    renderModalMessages();
  }

  // Prepend to visible deliveries list if matches current filters
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

// Click on consumer card in column 4 opens modal
els.consumers.addEventListener('click', (ev) => {
  const card = ev.target.closest('[data-consumer-name]');
  if (card && card.dataset.consumerName) {
    openConsumerModal(card.dataset.consumerName);
  }
});

// Modal close button
els.modal.btnClose.addEventListener('click', closeConsumerModal);

// Close on clicking backdrop outside modal dialog
els.modal.backdrop.addEventListener('click', (ev) => {
  if (ev.target === els.modal.backdrop) {
    closeConsumerModal();
  }
});

// Close on Escape key
window.addEventListener('keydown', (ev) => {
  if (ev.key === 'Escape' && els.modal.backdrop.style.display !== 'none') {
    closeConsumerModal();
  }
});

// Refresh button inside modal
els.modal.btnRefresh.addEventListener('click', reloadModalDeliveries);

// Live search inside modal
els.modal.search.addEventListener('input', () => {
  renderModalMessages();
});

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
