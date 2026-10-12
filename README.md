# ◆ Kurir: a tiny message broker

Kurir is a simple **message broker** built from scratch in Node.js for the Cloud Computing course.

**Producers** send messages labelled with a **routing key** (e.g. `order.paid`). The broker
uses **direct routing** to copy each message into every **queue** bound to that exact key.
**Consumers** then read from those queues. A **live web dashboard** shows every message
moving from producer → exchange → queue → consumer.

Features:
- Direct routing (exact routing-key match) to one or many queues
- Round-robin load sharing when several consumers read the same queue
- Messages wait in the queue while no consumer is connected
- Unroutable messages (no matching queue) are detected and dropped
- Live animated dashboard with a publish form and event log
- Zero dependencies: only built-in Node.js modules

> 📘 New to the project? Read **[CODEBASE_GUIDE.md](CODEBASE_GUIDE.md)** for a full explanation
> of the concepts and every file.

---

## Requirements

- [Node.js](https://nodejs.org/) **18 or newer** (check with `node -v`)
- No `npm install` needed

## How to run

**1. Start the broker** (keep this terminal open):

```bash
npm start
```

This starts:
- the broker on `tcp://localhost:4000` (for producers & consumers)
- the dashboard at **http://localhost:8080**

**2. Open the dashboard** in your browser: http://localhost:8080

**3. Generate traffic.** Pick either option.

**Option A: run the demo** (in a new terminal):

```bash
npm run demo
```

This starts a fake online shop with 2 producers and 4 consumers that publishes a message every
1.5 seconds. Press `Ctrl+C` to stop. For a different speed, give the interval in milliseconds:
`node clients/demo.js 500`.

**Option B: run producers and consumers yourself** (each in its own terminal):

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
| `npm start` | Start the broker + dashboard directly |
| `npm run demo` | Run the demo workload |
| `npm run producer -- [routingKey] [payload] [--name NAME]` | Send messages |
| `npm run consumer -- <queue> <keys...> [--name NAME]` | Receive messages |

## Project structure

```
Dockerfile          container build definition (Node 22 Alpine, zero dependencies)
docker-compose.yml  compose file attaching to existing tunnel_net network
src/                broker server (protocol.js, broker.js, server.js)
clients/            client library, producer & consumer CLIs, demo
public/             dashboard (index.html, style.css, app.js)
```

