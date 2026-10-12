# ◆ Kurir: a tiny message broker

Kurir is a simple **message broker** built from scratch in Node.js for the Cloud Computing course.

**Producers** send messages labelled with a **routing key** (e.g. `order.paid`). The broker
uses **direct routing** to copy each message into every **queue** bound to that exact key.
**Consumers** then read from those queues. A **live web dashboard** shows every message
moving from producer → exchange → queue → consumer.

Features:
- Direct routing (exact routing-key match) to one or many queues
- Round-robin load sharing when several consumers read the same queue
- **Persistent SQLite storage** (`node:sqlite`): message history, queue contents, and consumer deliveries survive restarts
- **Interactive Client Studio** (`http://localhost:3000`): visual producer and consumer dashboard so you don't have to type terminal commands
- **Consumer Processed Data Inspector** on the main dashboard (`http://localhost:8080`) with filterable history
- Unroutable messages (no matching queue) are detected and dropped
- Live animated dashboard with a publish form and event log
- Zero dependencies: only built-in Node.js modules

> 📘 New to the project? Read **[CODEBASE_GUIDE.md](CODEBASE_GUIDE.md)** for a full explanation
> of the concepts and every file.

---

## Requirements

- [Node.js](https://nodejs.org/) **22 or newer** (for native `node:sqlite` support, check with `node -v`)
- No `npm install` needed

## How to run

**1. Start the broker** (keep this terminal open):

```bash
npm start
```

This starts:
- the broker on `tcp://localhost:4000` (for producers & consumers)
- the main dashboard at **http://localhost:8080**
- persistent SQLite storage at `data/kurir.db`

**2. Open the Kurir dashboard** in your browser: http://localhost:8080

**3. Choose how you want to interact:**

### Option A: Local Client Studio (Visual UI — No Terminal Typing!)
Open a second terminal and run:

```bash
npm run client
```

Now open **http://localhost:3000** in your browser:
- **Publish messages** with 1-click presets (`order.created`, `order.paid`, `user.registered`, `order.refunded`) or enter custom JSON.
- **Spawn consumers** visually by specifying queue and routing keys.
- **Live consumer inbox**: watch messages processed by your consumers in real time!
- **Auto-traffic toggle**: click "Start Auto Traffic" to generate continuous traffic.

### Option B: Automated Shop Demo (Terminal)

```bash
npm run demo
```

This starts a fake online shop with 2 producers and 4 consumers that publishes a message every
1.5 seconds. Press `Ctrl+C` to stop. For a different speed, pass milliseconds: `node clients/demo.js 500`.

### Option C: Run Producers and Consumers via CLI

```bash
# Consumer: queue name, then one or more routing keys to bind
npm run consumer -- billing order.created order.paid --name billing-1

# A second consumer on the same queue (they take turns)
npm run consumer -- billing order.created --name billing-2

# Another queue
npm run consumer -- shipping order.paid --name shipping-svc

# Producer, interactive mode: type "<routingKey> <payload>" and press Enter
npm run producer -- --name web-shop
kurir> order.paid {"orderId": 1, "method": "gopay"}
kurir> order.created {"orderId": 2}
kurir> nobody.listens hello
```

Send a single message and exit:

```powershell
# PowerShell (Windows)
node clients/producer.js order.paid '{\"orderId\": 7}'
```

```bash
# macOS / Linux
node clients/producer.js order.paid '{"orderId": 7}'
```

You can also publish from the **"Publish a message"** panel on the dashboard.

## Docker & Cloudflare Tunnel (NixOS)

This project includes a [Dockerfile](Dockerfile) and [docker-compose.yml](docker-compose.yml) configured to reuse an existing Cloudflare Tunnel container running on the shared `tunnel_net` Docker bridge network.

### 1. Run on NixOS

```bash
# In the project directory on your NixOS server:
docker compose up -d --build
```

The container `kurir-broker` will:
- Attach to the shared `tunnel_net` network (resolvable via Docker DNS by your `cloudflared` container)
- Expose TCP port `4000:4000` to the NixOS host for producers and consumers

### 2. Add Route in Cloudflare Zero Trust

In the Cloudflare Zero Trust Dashboard:
1. Go to **Networks** → **Tunnels** → select your active tunnel.
2. Under **Public Hostname**, click **Add a public hostname**.
3. Fill in:
   - **Subdomain / Domain**: e.g., `kurir.aselaboratory.com` (or your preferred subdomain)
   - **Type**: `HTTP`
   - **URL**: `kurir-broker:8080`
4. Click **Save hostname**.

You can now open `https://kurir.aselaboratory.com` in your browser from anywhere to see the live dashboard!

### 3. Connect TCP Clients on the Server

```bash
# Run demo workload on the server:
node clients/demo.js

# Or connect consumer to the broker:
node clients/consumer.js billing order.created --name worker-1
```

## Configuration

| Variable | Default | Used by |
| --- | --- | --- |
| `BROKER_PORT` | `4000` | broker, clients |
| `DASHBOARD_PORT` | `8080` | broker |
| `BROKER_HOST` | `127.0.0.1` | clients |
| `HOST` | `0.0.0.0` | broker |

## Commands

| Command | Description |
| --- | --- |
| `npm start` | Start the broker + main dashboard (port 8080) |
| `npm run client` | Start the visual Client Studio (port 3000) |
| `npm run demo` | Run the automated shop demo workload |
| `npm run producer -- [routingKey] [payload] [--name NAME]` | Send messages via CLI |
| `npm run consumer -- <queue> <keys...> [--name NAME]` | Receive messages via CLI |

## Project structure

```
Dockerfile          container build definition (Node 22 Alpine, zero dependencies)
docker-compose.yml  compose file attaching to existing tunnel_net network with persistent data volume
src/
  db.js             persistent SQLite storage (node:sqlite)
  protocol.js       NDJSON wire framing
  broker.js         broker core (routing, queues, delivery)
  server.js         TCP :4000 server & HTTP :8080 dashboard
clients/
  client.js         TCP client library
  client-dashboard.js local Client Studio web server (:3000)
  ui/               Client Studio visual frontend
  producer.js       CLI producer
  consumer.js       CLI consumer
  demo.js           demo shop simulator
public/             main broker dashboard (:8080) with consumer history viewer
data/               SQLite database directory (git-ignored)
```

