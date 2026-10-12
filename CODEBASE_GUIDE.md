# 📘 Kurir: The Complete Codebase Guide

This document explains **everything** in this project, from scratch. You don't need to
know anything beforehand. Read it top to bottom the first time.

> **How to use this guide**
> - **Part 1** explains the *idea* of a message broker (no code).
> - **Part 2** shows the *big picture*: which programs run and how they talk.
> - **Part 3** is a *map* of the folders and what order to read them in.
> - **Part 4** walks through *every file*, piece by piece.
> - **Part 5** follows *one single message* through the whole system, step by step.
> - **Part 6–8** are references: protocol, JavaScript features used, and FAQ.
>
> Tip: keep the dashboard (`http://localhost:8080`) open while reading. Seeing it move helps.

---

## Table of contents

1. [Part 1: What is a message broker?](#part-1-what-is-a-message-broker)
2. [Part 2: The big picture](#part-2-the-big-picture)
3. [Part 3: Folder map & reading order](#part-3-folder-map--reading-order)
4. [Part 4: File-by-file walkthrough](#part-4-file-by-file-walkthrough)
   - [4.1 package.json](#41-packagejson)
   - [4.2 src/protocol.js: how bytes become messages](#42-srcprotocoljs-how-bytes-become-messages)
   - [4.3 src/broker.js: the brain](#43-srcbrokerjs-the-brain)
   - [4.4 src/server.js: the doors](#44-srcserverjs-the-doors)
   - [4.5 clients/client.js: the client library](#45-clientsclientjs-the-client-library)
   - [4.6 clients/producer.js, consumer.js, demo.js](#46-clientsproducerjs-consumerjs-demojs)
   - [4.7 public/: the dashboard](#47-public-the-dashboard)
5. [Part 5: Follow one message end-to-end](#part-5-follow-one-message-end-to-end)
6. [Part 6: Protocol reference](#part-6-protocol-reference)
7. [Part 7: JavaScript features you'll see](#part-7-javascript-features-youll-see)
8. [Part 8: FAQ, limitations & how to extend](#part-8-faq-limitations--how-to-extend)

---

## Part 1: What is a message broker?

### The problem

Imagine an online shop made of several separate programs ("services"):

- **web-shop**: the website where people place orders
- **billing**: charges money
- **shipping**: sends packages
- **mailer**: sends emails

When someone places an order, the web-shop needs to tell billing *and* mailer. When someone
pays, it needs to tell billing *and* shipping.

**Without a broker**, the web-shop must know the address of every service and call each one
directly. If shipping is down, the call fails and the message is lost. If you add a new
service, you must change the web-shop's code.

### The solution: a post office in the middle

A **message broker** is like a **post office**:

| Post office | Message broker | In this project |
| --- | --- | --- |
| Person sending a letter | **Producer** | `web-shop`, `mobile-app`, `dashboard` |
| The letter's contents | **Payload** | `{"orderId": 42}` |
| The label "Department: Billing" on the envelope | **Routing key** | `order.paid` |
| The sorting desk | **Exchange** | the "DIRECT" circle on the dashboard |
| The sorting rules ("Billing-labelled letters go to box B") | **Bindings** / routing table | `order.paid → billing, shipping` |
| A P.O. box that holds letters | **Queue** | `billing`, `shipping`, `email` |
| The person who picks up letters from the box | **Consumer** | `billing-1`, `shipping-svc`, `mailer` |

> **Important correction to a common misunderstanding:**
> The broker does **not** open the letter and read the contents. It only reads the
> **label on the envelope** (the routing key). The producer decides the label.
> The payload is passed along untouched.

### The 4 rules of *this* broker (direct routing)

1. **A queue is "bound" to one or more routing keys.**
   e.g. the `billing` queue says: "send me `order.created` and `order.paid`."
2. **When a message arrives, it is copied into EVERY queue bound to that exact key.**
   `order.paid` is bound to both `billing` and `shipping`, so both get their own copy.
   ("Direct" = the key must match *exactly*, letter for letter.)
3. **Inside one queue, messages are given out to consumers in turn (round-robin).**
   If `billing-1` and `billing-2` both read from `billing`, message 1 → billing-1,
   message 2 → billing-2, message 3 → billing-1… They *share the work*.
4. **If a queue has no consumer, messages wait in it.** When a consumer connects later,
   it receives all of them.
   **If no queue is bound to a key at all, the message is "unroutable" and thrown away.**

### Why this is useful (the three big benefits)

| Benefit | Meaning | Where you see it in the demo |
| --- | --- | --- |
| **Decoupling** | Producers don't know who the consumers are, and vice versa. | `web-shop` never mentions `billing` in its code. |
| **Buffering** | If a consumer is offline, messages wait instead of being lost. | Stop a consumer → its queue fills up on the dashboard. |
| **Load sharing** | Several workers can split the work of one queue. | `billing-1` and `billing-2` alternate. |

These ideas come from real brokers like **RabbitMQ** (which uses the same words: exchange,
binding, queue, routing key). Kurir is a tiny educational version of it.

---

## Part 2: The big picture

When everything is running there are **several separate programs (processes)**:

```
                              ┌────────────────────────────────────────────┐
                              │  BROKER PROCESS   (npm start → server.js)  │
                              │                                            │
  ┌──────────────┐  TCP :4000 │  ┌──────────────┐      ┌───────────────┐   │
  │  producer.js │◀──────────▶│  │  TCP server  │─────▶│               │   │
  └──────────────┘            │  └──────────────┘      │               │   │
  ┌──────────────┐  TCP :4000 │         ▲              │   Broker      │   │
  │  consumer.js │◀──────────▶│─────────┘              │   (broker.js) │   │
  └──────────────┘            │                        │               │   │
  ┌──────────────┐  TCP :4000 │                        │  - queues     │   │
  │   demo.js    │◀──────────▶│                        │  - bindings   │   │
  └──────────────┘            │                        │  - clients    │   │
                              │  ┌──────────────┐      │               │   │
  ┌──────────────┐ HTTP :8080 │  │ HTTP server  │◀────▶│               │   │
  │   Browser    │◀──────────▶│  │ (dashboard)  │      └───────────────┘   │
  │ (public/*)   │            │  └──────────────┘                          │
  └──────────────┘            └────────────────────────────────────────────┘
```

- **One broker process** (`src/server.js`) holds all the state. It has **two "doors"**:
  - **Port 4000 (TCP)**: producers and consumers connect here and exchange messages.
  - **Port 8080 (HTTP)**: your web browser connects here to see the dashboard.
- **Client processes** (`producer.js`, `consumer.js`, `demo.js`) are *separate programs*.
  They could even run on a different computer. They only talk to the broker, never to each other.
- **The browser** loads the dashboard (HTML/CSS/JS) and receives a live stream of
  everything the broker does, which it turns into animations.

### What's TCP? What's a port?

- A **port** is like an apartment number on a computer. The computer is the building
  (`localhost` = this computer), and different programs listen at different numbers.
- **TCP** is the basic way two programs send bytes to each other over a network reliably
  and in order. HTTP (what websites use) is built *on top of* TCP. We use raw TCP for the
  broker because it keeps a connection open, so the broker can *push* messages to consumers
  at any time.

---

## Part 3: Folder map & reading order

```
Message Project Project/
├── package.json          ← project info + shortcut commands (npm start, npm run demo…)
├── README.md             ← short description + how to run
├── CODEBASE_GUIDE.md     ← this file
│
├── src/                  ← THE BROKER (server side)
│   ├── protocol.js       ← ① how messages are packaged into bytes on the wire
│   ├── broker.js         ← ② the brain: queues, bindings, routing   ⭐ most important
│   └── server.js         ← ③ opens ports 4000 & 8080 and connects them to the brain
│
├── clients/              ← PROGRAMS THAT USE THE BROKER
│   ├── client.js         ← ④ small helper library to connect/publish/subscribe
│   ├── producer.js       ← ⑤ command-line tool to SEND messages
│   ├── consumer.js       ← ⑤ command-line tool to RECEIVE messages
│   └── demo.js           ← ⑥ fake online shop that generates traffic automatically
│
└── public/               ← THE DASHBOARD (runs in your browser)
    ├── index.html        ← ⑦ page structure
    ├── style.css         ←   page looks
    └── app.js            ← ⑧ receives live events and animates them
```

**Recommended reading order:** follow the numbers ① → ⑧. If you only have time for one
file, read **`src/broker.js`**. It's the actual message broker. Everything else is
plumbing (networking) or presentation (dashboard).

> **There are no external libraries.** Everything uses only what's built into Node.js
> (`net`, `http`, `fs`, `events`, `crypto`…). That's why there's no `node_modules` folder
> and no `npm install` step.

---

## Part 4: File-by-file walkthrough

### 4.1 `package.json`

[package.json](package.json) describes the project for Node.js / npm. The important part is
`scripts`, which are just shortcuts:

| You type | It actually runs | What it does |
| --- | --- | --- |
| `npm start` | `node src/server.js` | Starts the broker + dashboard |
| `npm run demo` | `node clients/demo.js` | Starts the fake shop |
| `npm run producer -- …` | `node clients/producer.js …` | Sends messages |
| `npm run consumer -- …` | `node clients/consumer.js …` | Receives messages |

The `--` means "pass everything after this to the script, not to npm."

---

### 4.2 `src/protocol.js`: how bytes become messages

[src/protocol.js](src/protocol.js) is small but solves a real problem.

#### The problem: TCP has no "messages"

TCP gives you a **stream of bytes**, like water through a pipe. If a client sends two
messages quickly:

```
{"type":"publish","routingKey":"a"}   and   {"type":"publish","routingKey":"b"}
```

the receiver might get them as **one chunk**, or **split in weird places**:

```
chunk 1:  {"type":"publish","rout
chunk 2:  ingKey":"a"}{"type":"publish","routingKey":"b"}
```

So we need a rule for *where one message ends*. This is called **framing**.

#### The solution: one JSON object per line (NDJSON)

Our rule: **every message is one line of JSON, ending with `\n` (newline).**
JSON never contains a raw newline inside it, so a newline always means "message ends here."

```
{"type":"publish","routingKey":"a"}\n{"type":"publish","routingKey":"b"}\n
```

#### The three functions

**[`attachJsonLines(socket, onMessage)`](src/protocol.js#L15)**: the *receiver* side.

```js
let buffer = '';                         // leftover bytes that aren't a full line yet
socket.on('data', (chunk) => {           // runs every time bytes arrive
  buffer += chunk;                       // add new bytes to leftovers
  while ((newlineIndex = buffer.indexOf('\n')) !== -1) {   // is there a full line?
    const line = buffer.slice(0, newlineIndex);            // cut it out
    buffer = buffer.slice(newlineIndex + 1);               // keep the rest
    msg = JSON.parse(line);                                // text → JS object
    onMessage(null, msg);                                  // hand it to whoever asked
  }
});
```

Think of it as: *"collect bytes in a bucket; every time the bucket contains a full line,
scoop it out, parse it, and report it."* There's also a safety check: if 1 MB arrives
without any newline, the connection is closed (protects against bad clients).

**[`send(socket, obj)`](src/protocol.js#L51)**: the *sender* side. Turns an object into
JSON text, adds `\n`, writes it to the socket.

**[`parsePayload(text)`](src/protocol.js#L58)**: helper for the command-line tools. If you
type `{"orderId": 1}` it becomes a real object; if you type `hello` it stays the string `"hello"`.

Both the broker (server) **and** the clients use this file, so they speak the same language.

---

### 4.3 `src/broker.js`: the brain

[src/broker.js](src/broker.js) **is** the message broker. Notice it contains **no networking
code at all**. It doesn't know about TCP, HTTP, or browsers. It just manages data in memory.
This separation makes it easy to understand: you can read it like a pure "rules" file.

#### The data it keeps (in the [constructor](src/broker.js#L26))

```js
this.queues   = new Map();  // queueName  → queue object
this.bindings = new Map();  // routingKey → Set of queue names   ← the ROUTING TABLE
this.clients  = new Map();  // clientId   → client object
this.stats    = { published: 0, routed: 0, delivered: 0, unroutable: 0 };
```

> A `Map` is a dictionary/lookup table: give it a key, get a value.
> A `Set` is a list with no duplicates.

Here is what that data looks like **while the demo is running**:

```
clients:
  "a1b2c3d4" → { name: "web-shop",   role: "producer", queues: {} }
  "e5f6a7b8" → { name: "billing-1",  role: "consumer", queues: {"billing"} }
  "c9d0e1f2" → { name: "billing-2",  role: "consumer", queues: {"billing"} }
  ...

queues:
  "billing"  → { bindingKeys: {"order.created","order.paid"},
                 messages:   [ ...waiting messages... ],
                 consumers:  ["e5f6a7b8", "c9d0e1f2"],     ← billing-1, billing-2
                 nextConsumer: 0 }                          ← whose turn it is
  "shipping" → { bindingKeys: {"order.paid"}, messages: [], consumers: [...], ... }
  "email"    → { bindingKeys: {"user.registered","order.created"}, ... }

bindings  (the routing table, the exchange's "sorting rules"):
  "order.created"   → {"billing", "email"}
  "order.paid"      → {"billing", "shipping"}
  "user.registered" → {"email"}
```

Notice that bindings are stored **twice**: on each queue (`bindingKeys`) and in the
`bindings` map. The `bindings` map exists so that routing is a single fast lookup:
"who wants `order.paid`?" → `bindings.get("order.paid")`.

#### Each method, explained

**[`registerClient({ name, role, deliver })`](src/broker.js#L45)**: called when a new
program connects. Gives it a random 8-character ID and remembers it.
The interesting part is **`deliver`**: it's a *function* the network layer gives the broker
meaning "call this when you want to send a message to this client." This is how the broker
can send messages over TCP **without knowing anything about TCP**.

**[`removeClient(clientId)`](src/broker.js#L60)**: called when a program disconnects. Removes
it from every queue's consumer list. **The queues and their bindings stay**, so messages
keep piling up in them until someone reconnects (that's *buffering*).

**[`declareQueue(name)`](src/broker.js#L74)**: "make sure a queue with this name exists."
If it already exists, nothing happens. (Calling it twice is safe.)

**[`bindQueue(queueName, routingKey)`](src/broker.js#L85)**: adds a sorting rule:
"messages with this key should go into this queue." Updates both the queue's
`bindingKeys` and the global `bindings` table.

**[`subscribe(clientId, queueName, bindingKeys)`](src/broker.js#L97)**: what a consumer
calls. It does three things in one go:
1. creates the queue if needed,
2. binds each key to it,
3. adds the client to the queue's consumer list.

Then it calls `#drain` so any messages that were already waiting get delivered.
(It uses `queueMicrotask` to do that *a moment later*, so the consumer first receives the
"subscribed" confirmation and *then* the waiting messages, which is a nicer order.)

**[`publish(clientId, routingKey, payload)`](src/broker.js#L119)** ⭐: **the heart of the broker.**

```js
// Build the message "envelope"
const message = { id, routingKey, payload, publishedBy: client.name, timestamp: Date.now() };

// 1. Look up the routing table: which queues want this key?
const targets = [...(this.bindings.get(routingKey) ?? [])];

// Nobody wants it → unroutable, drop it
if (targets.length === 0) { this.stats.unroutable++; ...; return { id, routedTo: [] }; }

// 2. Put a copy into every matching queue
for (const queueName of targets) {
  this.queues.get(queueName).messages.push(message);
}

// 3. Try to deliver right now
for (const queueName of targets) this.#drain(this.queues.get(queueName));

return { id: message.id, routedTo: targets };   // tell the producer where it went
```

Notice `payload` is **never looked at**, only stored and passed along.

**[`#drain(queue)`](src/broker.js#L162)** ⭐: delivers waiting messages. The `#` means it's
*private* (only the broker itself can call it).

```js
while (queue.messages.length > 0 && queue.consumers.length > 0) {   // something to give AND someone to give it to
  const index = queue.nextConsumer % queue.consumers.length;        // whose turn? (% wraps around)
  queue.nextConsumer = index + 1;                                   // next time, the next one
  const consumer = this.clients.get(queue.consumers[index]);
  const message  = queue.messages.shift();                          // take the OLDEST message (first in, first out)
  consumer.deliver(message, queue.name);                            // actually send it
}
```

This loop is where **round-robin** happens. With 2 consumers, `index` goes 0, 1, 0, 1…
The `%` (modulo, "remainder after division") makes it wrap around.
If there are no consumers, the loop doesn't run and messages simply stay in `queue.messages`.

**[`snapshot()`](src/broker.js#L183)**: returns a plain copy of the whole state. The
dashboard uses this when it first opens, to draw everything that already exists.

**[`#emit(type, data)`](src/broker.js#L199)**: every time anything happens, the broker
announces it as an **event**, e.g.:

```js
{ type: 'message.routed', ts: 1760287000000, stats: {...}, messageId: 'f21f7311', queue: 'billing', depth: 1 }
```

The broker doesn't care who listens. Right now, two things listen: the console logger and
the dashboard. The full list of events:

| Event | When |
| --- | --- |
| `client.connected` / `client.disconnected` | a program connects / leaves |
| `queue.declared` | a new queue is created |
| `queue.bound` | a routing key is bound to a queue |
| `consumer.subscribed` | a consumer starts reading a queue |
| `message.published` | a producer sent a message |
| `message.routed` | a copy was put into a queue |
| `message.unroutable` | no queue matched, message dropped |
| `message.delivered` | a message was handed to a consumer |

**Helpers at the bottom:**
- [`assertName`](src/broker.js#L206) rejects empty or too-long names (input validation).
- [`publicClient`](src/broker.js#L211) / [`publicQueue`](src/broker.js#L215) convert internal
  objects into plain JSON-friendly objects (e.g. `Set` → array, and they hide the `deliver` function).

---

### 4.4 `src/server.js`: the doors

[src/server.js](src/server.js) is the program you start with `npm start`. It creates **one**
`Broker` and connects it to the outside world through two servers.

#### Part A: TCP server (port 4000) for producers & consumers

```js
const tcpServer = net.createServer((socket) => { ... });
```

The function inside runs **once for every new connection**. `socket` is that connection,
which you can read from and write to. Each connection has its own `client` variable
(starts as `null`).

Every incoming line goes to [`handleCommand(msg)`](src/server.js#L48), which is basically a
switch on `msg.type`:

| Incoming command | What the server does | Reply sent back |
| --- | --- | --- |
| `hello` | `broker.registerClient(...)`, giving it a `deliver` function that writes to *this* socket | `welcome` with the client's ID |
| `publish` | `broker.publish(...)` | `published` with `routedTo` |
| `subscribe` | `broker.subscribe(...)` | `subscribed` |
| anything else | throw an error | `error` |

A client **must** say `hello` first. Otherwise it gets an error. Any error thrown while
handling a command is caught and sent back as `{"type":"error", ...}` instead of crashing
the broker.

The key line that connects the broker brain to the network:

```js
deliver: (message, queue) => send(socket, { type: 'message', queue, message }),
```

"When the broker wants to deliver to this client, write a `message` line to this socket."

When the socket closes (program quit, Ctrl+C, crash), `broker.removeClient` is called.

#### Part B: HTTP server (port 8080) for the dashboard

```js
const httpServer = http.createServer((req, res) => { ... });
```

Runs once per HTTP request. It looks at the URL and decides what to do:

| Request | Handler | Purpose |
| --- | --- | --- |
| `GET /events` | [`handleEvents`](src/server.js#L106) | Live stream of broker events (SSE, see below) |
| `GET /api/state` | `broker.snapshot()` | Current state as JSON (handy for debugging) |
| `POST /api/publish` | [`handlePublish`](src/server.js#L129) | The dashboard's "Send to broker" button |
| `GET /anything-else` | [`serveStatic`](src/server.js#L146) | Sends files from `public/` (HTML, CSS, JS) |

**What is SSE (Server-Sent Events)?**
Normally HTTP is "browser asks → server answers → done." SSE keeps the response **open
forever** and the server keeps writing to it. Each event looks like:

```
event: broker
data: {"type":"message.published", ...}

```

The browser has a built-in `EventSource` object that reads this format. In `handleEvents`:

1. send headers saying "this is an event stream",
2. send one `snapshot` event (the full current state),
3. subscribe to the broker's events with `broker.on('event', ...)` and forward each one,
4. send a `: ping` comment every 15 s so the connection doesn't time out,
5. when the browser tab closes, unsubscribe (`broker.off`) to avoid leaks.

**The dashboard as a producer:** at startup the server registers a client named
`dashboard`. When you press "Send to broker", `handlePublish` publishes *as that client*.
That's why "dashboard" appears in the Producers column.

**Security note in `serveStatic`:** the check
`if (!filePath.startsWith(PUBLIC_DIR + path.sep))` stops someone from requesting
`/../src/server.js` to read files outside `public/` (a "path traversal" attack).

#### Part C: Console log

`broker.on('event', ...)` prints a colored line for each event in the terminal. The odd
`\x1b[36m` strings are **ANSI color codes** (they tell the terminal "switch to cyan").

#### Part D: Start

Start the TCP server, then the HTTP server, then print the banner. If a port is already in
use (e.g. you started the broker twice), `onListenError` prints a clear message and exits.

---

### 4.5 `clients/client.js`: the client library

[clients/client.js](clients/client.js) hides the networking details so the other client
programs stay simple. Usage:

```js
const client = await connect({ name: 'web-shop', role: 'producer' });
client.publish('order.created', { orderId: 42 });

const worker = await connect({ name: 'billing-1', role: 'consumer' });
worker.subscribe('billing', ['order.created']);
worker.on('message', ({ queue, message }) => console.log(message.payload));
```

How [`connect()`](clients/client.js#L23) works:

1. Opens a TCP connection to `127.0.0.1:4000` (`net.createConnection`).
2. As soon as it's connected, sends `{"type":"hello", name, role}`.
3. Returns a **Promise** that *resolves* (finishes successfully) when the broker replies
   `welcome`. That's why you write `await connect(...)`: "wait until we're officially in."
4. Every line received from the broker is re-announced as an event with the same name:
   a `message` line → `client.on('message', ...)`, a `published` line → `client.on('published', ...)`.
   (Exception: `error` is renamed to `brokerError`, because in Node.js an unhandled
   `'error'` event crashes the program.)

`publish()` and `subscribe()` are one-liners that `send()` the right JSON.

---

### 4.6 `clients/producer.js`, `consumer.js`, `demo.js`

These are small command-line programs built on `client.js`.

**[producer.js](clients/producer.js)** has two modes:

- **One-shot:** `node clients/producer.js order.paid '{"orderId":7}'`
  → connect, publish once, wait for the `published` reply, print where it went, exit.
- **Interactive:** `node clients/producer.js --name web-shop`
  → shows a `kurir>` prompt (using Node's built-in `readline`). Each line you type is split
  at the first space into `routingKey` + `payload` and published.

The output tells you the routing result:
`✔ routed to: billing, shipping` or `⚠ unroutable: no queue is bound to that key`.

**[consumer.js](clients/consumer.js)**:
`node clients/consumer.js billing order.created order.paid --name billing-1`

- first word = queue name, remaining words = binding keys, `--name` = display name.
- connects as a consumer, sends `subscribe`, then prints every `message` it receives with a
  counter (`#1`, `#2`, …).

**[demo.js](clients/demo.js)** simulates an online shop **inside one program**:

- connects **4 consumers**: `billing-1` and `billing-2` (same queue → round-robin),
  `shipping-svc`, and `mailer`;
- connects **2 producers**: `web-shop` and `mobile-app`;
- every 1.5 s, picks a random event and a random producer and publishes it.

The events are **weighted** (`order.created` appears 4× as often as `order.refunded`).
`order.refunded` is **deliberately not bound to any queue**, so you can see unroutable
messages too. You can change the speed: `node clients/demo.js 500` (every 500 ms).

> Even though the demo runs everything in one program, each producer/consumer opens its
> **own TCP connection**, so to the broker they are completely separate clients, exactly as
> if they were separate programs on separate machines.

---

### 4.7 `public/`: the dashboard

The dashboard is **only a viewer** (plus a publish form). The broker works perfectly fine
without it. These three files are sent to your browser by `serveStatic`.

#### [index.html](public/index.html): structure

- **Top bar:** logo and the connection badge ("Live" / "Reconnecting…").
- **Stats row:** Published, Routed copies, Delivered, Unroutable, Waiting in queues.
- **Stage** (the big panel) with 4 columns that follow the message path:
  `1 Producers → 2 Exchange → 3 Queues → 4 Consumers`.
  The Exchange column also shows the **routing table** and the **Unroutable** bin.
  An invisible layer on top (`#fx-layer`) is where the flying dots are drawn.
- **Side panel:** the publish form and the event log.

#### [style.css](public/style.css): looks

Pure styling, no logic. Worth knowing:
- CSS variables at the top (`--violet`, `--teal`, …) define the color palette.
- `.glass` is the frosted-glass panel look (`backdrop-filter: blur`).
- `@keyframes` blocks define animations (`spin` for the exchange ring, `hit` for the
  flash when a card receives a message, `bump` for stat numbers).
- `@media` blocks rearrange the layout on smaller screens.

#### [app.js](public/app.js): logic

It does 3 jobs: **(1) keep a copy of the broker state, (2) draw it, (3) animate events.**

**1. Connecting:** [`connect()`](public/app.js#L495)

```js
const source = new EventSource('/events');
source.addEventListener('snapshot', (ev) => applySnapshot(JSON.parse(ev.data)));  // full state, once
source.addEventListener('broker',   (ev) => handleEvent(JSON.parse(ev.data)));    // every change after
```

If the broker restarts, `EventSource` reconnects automatically, gets a fresh snapshot, and
redraws everything.

**2. Local state:** a `state` object mirrors the broker (`clients`, `queues`, `bindings`,
`stats`). [`applySnapshot`](public/app.js#L370) fills it from scratch.
[`handleEvent`](public/app.js#L483) updates it bit by bit using the
[`handlers`](public/app.js#L384) object, which has one function per event type
(`'client.connected'`, `'queue.bound'`, …).

**3. Drawing:** `renderClients`, `renderQueues`, `renderRoutingTable`, etc. build the HTML.
They use a helper called [`reconcile`](public/app.js#L97), which updates a list **without
rebuilding it from scratch**: it reuses existing cards, adds new ones, removes old ones.
That's why cards don't flicker when something changes.

**4. Animation:** this is the trickiest part, and here's why.

The broker processes a message in **less than a millisecond**: `published`, `routed`,
`routed`, `delivered`, `delivered` events all arrive at practically the same instant. If we
animated them as they arrive, all the dots would fly at once and you couldn't follow the path.

The fix: **each hop waits for the previous hop to finish**, using Promises:

```
message.published  → fly dot: producer → exchange          (store promise under "msgId")
message.routed     → WAIT for "msgId",           then fly: exchange → queue
                                                  (store promise under "msgId:queue")
message.delivered  → WAIT for "msgId:queue",     then fly: queue → consumer
```

- [`fly(fromEl, toEl, color)`](public/app.js#L300) creates a small glowing dot, moves it
  from the center of one element to another in a curved path using the browser's
  `element.animate()` API, and returns a Promise that finishes when the dot lands.
- [`track(key, promise)`](public/app.js#L327) stores the promise in the `pending` map so
  the next hop can wait for it (`after(key)`).
- When a message is routed to a queue that has **no consumer**, it lands and stays as a
  little colored square inside the queue card (`addItem`). When it's delivered later, the
  square is removed (`removeItem`) and the dot flies to the consumer.
- `deliveredEarly` is a small detail: if a message is delivered immediately, we skip placing
  it in the queue so the square doesn't flash for a split second.

**Colors:** [`colorFor(routingKey)`](public/app.js#L77) turns a routing key into a color
using a tiny hash, so `order.paid` is **always the same color** everywhere (dot, chip,
routing-table row, log). This lets you follow a key with your eyes.

**5. Publish form:** [the submit handler](public/app.js#L513) sends
`POST /api/publish` with `{ routingKey, payload }` using `fetch`, and shows the answer
("Routed to billing, email" or "dropped").

---

## Part 5: Follow one message end-to-end

Let's trace exactly what happens when `web-shop` publishes `order.paid`, assuming the demo
setup (billing has 2 consumers, shipping has 1).

**① web-shop sends one line over TCP** (via `client.publish` → `send`):

```json
{"type":"publish","routingKey":"order.paid","payload":{"orderId":6807,"method":"card"}}
```

**② Broker receives bytes** → `attachJsonLines` finds the `\n`, parses the JSON →
`handleCommand` sees `type: "publish"` → calls `broker.publish(webShopId, "order.paid", {...})`.

**③ Broker builds the envelope:**

```json
{ "id": "f21f7311", "routingKey": "order.paid", "payload": {"orderId":6807,"method":"card"},
  "publishedBy": "web-shop", "timestamp": 1760287000000 }
```

and emits `message.published`.

**④ Routing table lookup:** `bindings.get("order.paid")` → `{"billing", "shipping"}`.

**⑤ Copy into each queue:** the message is pushed into `billing.messages` and
`shipping.messages`. Two `message.routed` events are emitted.

**⑥ Drain `billing`:** it has consumers `[billing-1, billing-2]` and `nextConsumer` is,
say, 1 → it's **billing-2's turn**. The message is removed from the queue and
`deliver()` is called → the server writes to billing-2's socket:

```json
{"type":"message","queue":"billing","message":{"id":"f21f7311","routingKey":"order.paid","payload":{...},"publishedBy":"web-shop","timestamp":...}}
```

`nextConsumer` becomes 2, so next time `2 % 2 = 0` → billing-1.

**⑦ Drain `shipping`:** only one consumer → shipping-svc gets its copy.

**⑧ Reply to the producer:**

```json
{"type":"published","id":"f21f7311","routedTo":["billing","shipping"]}
```

**⑨ Meanwhile:** every event from ③–⑦ was also sent to the terminal log and (via SSE) to
every open dashboard, which animates:
web-shop → exchange → (billing, shipping) → (billing-2, shipping-svc).

**What if shipping-svc was offline?** Steps 1–6 are the same, but in step 7 the `#drain`
loop doesn't run (no consumers). The message stays in `shipping.messages`. When
shipping-svc reconnects and subscribes, `subscribe()` calls `#drain` and it receives the
message then.

**What if the key was `order.refunded`?** In step 4 the lookup finds nothing →
`message.unroutable` is emitted, the producer gets `routedTo: []`, and the message is gone.

---

## Part 6: Protocol reference

All communication on port 4000 is **one JSON object per line**.

**Client → Broker**

| Command | Example | Notes |
| --- | --- | --- |
| hello | `{"type":"hello","role":"consumer","name":"billing-1"}` | Must be first. `role` is `producer` or `consumer`. |
| publish | `{"type":"publish","routingKey":"order.paid","payload":{"orderId":1}}` | `payload` can be anything JSON. |
| subscribe | `{"type":"subscribe","queue":"billing","bindingKeys":["order.paid"]}` | Creates the queue & bindings if missing. |

**Broker → Client**

| Reply | Example |
| --- | --- |
| welcome | `{"type":"welcome","clientId":"a1b2c3d4","name":"billing-1"}` |
| published | `{"type":"published","id":"f21f7311","routedTo":["billing","shipping"]}` |
| subscribed | `{"type":"subscribed","queue":"billing","bindingKeys":["order.paid"]}` |
| message | `{"type":"message","queue":"billing","message":{"id":"…","routingKey":"…","payload":…,"publishedBy":"…","timestamp":…}}` |
| error | `{"type":"error","error":"Send {\"type\":\"hello\"} first"}` |

Because it's just text over TCP, a client could be written in **any language**
(Python, Java, Go…), as long as it sends and reads JSON lines.

**HTTP endpoints (port 8080)**

| Endpoint | Purpose |
| --- | --- |
| `GET /` | Dashboard page |
| `GET /events` | Live event stream (SSE) |
| `GET /api/state` | Current broker state as JSON |
| `POST /api/publish` | Body `{"routingKey":"…","payload":…}` → publish as "dashboard" |

---

## Part 7: JavaScript features you'll see

| Syntax | Meaning | Example from this code |
| --- | --- | --- |
| `require('node:net')` | Import a module (built-in ones start with `node:`) | `const net = require('node:net');` |
| `module.exports = {…}` | Make things available to other files | `module.exports = { Broker };` |
| `(a, b) => a + b` | Arrow function (short way to write a function) | `(socket) => { ... }` |
| `` `Hello ${name}` `` | Template string: put variables inside text | `` `client:${id}` `` |
| `class X extends Y` | Class that inherits from another | `class Broker extends EventEmitter` |
| `#name` | Private method/field, usable only inside the class | `#drain(queue)` |
| `EventEmitter` | Object you can `.on('x', fn)` and `.emit('x', data)` | broker emits `'event'` |
| `new Map()` / `new Set()` | Dictionary / unique list | `this.queues = new Map()` |
| `a ?? b` | "use `a`, but if it's null/undefined use `b`" | `this.bindings.get(key) ?? []` |
| `a?.b` | "get `b` only if `a` exists, otherwise undefined" | `msg?.ref` |
| `[...x]` | Spread: copy items of `x` into a new array | `[...this.queues.values()]` |
| `{ ...obj, a: 1 }` | Copy an object and add/override fields | `{ ...this.stats }` |
| `({ name, role })` | Destructuring: pull fields out of an object | `registerClient({ name, role, deliver })` |
| `Promise` | A value that will be ready later | `connect()` returns one |
| `async` / `await` | Write "wait for this Promise" code that reads top-to-bottom | `await connect(...)` |
| `try { } catch { }` | Handle errors without crashing | in `handleCommand` |
| `/** … */` | JSDoc comment: documentation (and type hints for your editor) | above most functions |

---

## Part 8: FAQ, limitations & how to extend

**Why raw TCP for clients instead of HTTP?**
HTTP is "request → response → done." A consumer needs the broker to *push* messages
whenever they arrive, so we need a connection that stays open in both directions. TCP
gives us that directly, and it shows how real brokers (RabbitMQ, Kafka) work: they all
use their own protocols over TCP.

**Why SSE for the dashboard instead of WebSockets?**
The dashboard only needs to *receive* a stream (publishing uses a normal POST). SSE is
built into browsers *and* trivial to write on the server with no libraries. WebSockets would
need either a library or a lot of protocol code.

**Is the order of messages guaranteed?**
Within one queue, yes: first in, first out (`push` to add at the end, `shift` to take
from the front).

**What happens when the broker restarts?**
Everything is in memory (JavaScript variables), so all queues, bindings, and waiting
messages are lost. Clients get disconnected.

**What happens if a consumer crashes while processing a message?**
The message is lost. The broker considers a message "done" the moment it *sends* it. Real
brokers solve this with **acknowledgements**.

### Limitations and where you'd change the code to fix them

| Limitation | Idea for improvement | Where in the code |
| --- | --- | --- |
| Messages lost on restart | Save queues to a file, reload on start | `broker.js`: `publish`, `#drain`, constructor |
| Lost if consumer crashes | **Acks**: keep message as "unacked" until the consumer replies `{"type":"ack","id":…}`; re-queue if it disconnects first | `broker.js` `#drain` + new `ack()`; `server.js` new command |
| Only exact-match routing | **Topic exchange**: `order.*` matches `order.paid`; `#` matches many words | `broker.js` `publish`: replace the `bindings.get()` lookup with pattern matching |
| Broadcast to everyone | **Fanout exchange**: ignore the key, send to all queues | `broker.js` `publish` |
| No security | Require a password/token in `hello` | `server.js` `handleCommand` |

---

*You now know every part of Kurir. A good final exercise: open `src/broker.js`, find
`publish()`, and try implementing the topic-exchange improvement above.*
