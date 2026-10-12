#!/usr/bin/env node
/**
 * Demo scenario: a small online shop.
 *
 *   Queues & bindings              Consumers
 *   ─────────────────              ─────────
 *   billing  ← order.created,      billing-1, billing-2  (round-robin)
 *              order.paid
 *   shipping ← order.paid          shipping-svc
 *   email    ← user.registered,    mailer
 *              order.created
 *
 * Producers "web-shop" and "mobile-app" publish random events. Note:
 *   • order.created goes to BOTH billing and email (one copy each)
 *   • order.refunded has no binding → unroutable (dropped)
 */

const { connect } = require('./client');

const CONSUMERS = [
  { name: 'billing-1', queue: 'billing', keys: ['order.created', 'order.paid'] },
  { name: 'billing-2', queue: 'billing', keys: ['order.created', 'order.paid'] },
  { name: 'shipping-svc', queue: 'shipping', keys: ['order.paid'] },
  { name: 'mailer', queue: 'email', keys: ['user.registered', 'order.created'] },
];

const PRODUCERS = ['web-shop', 'mobile-app'];

const rand = (n) => Math.floor(Math.random() * n);
const pick = (arr) => arr[rand(arr.length)];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const EVENTS = [
  { weight: 4, key: 'order.created', payload: () => ({ orderId: 1000 + rand(9000), total: (rand(50) + 1) * 10000 }) },
  { weight: 3, key: 'order.paid', payload: () => ({ orderId: 1000 + rand(9000), method: pick(['gopay', 'ovo', 'card']) }) },
  { weight: 2, key: 'user.registered', payload: () => ({ user: pick(['ayu', 'budi', 'citra', 'dimas', 'eka']) }) },
  { weight: 1, key: 'order.refunded', payload: () => ({ orderId: 1000 + rand(9000) }) },
];
const WEIGHTED = EVENTS.flatMap((e) => Array(e.weight).fill(e));

async function main() {
  const interval = Number(process.argv[2]) || 1500;

  for (const c of CONSUMERS) {
    const client = await connect({ name: c.name, role: 'consumer' });
    client.subscribe(c.queue, c.keys);
    client.on('message', ({ queue, message }) =>
      console.log(`  ${c.name.padEnd(13)} ← [${queue}] ${message.routingKey} ${JSON.stringify(message.payload)}`)
    );
    await sleep(150);
  }

  const producers = [];
  for (const name of PRODUCERS) producers.push(await connect({ name, role: 'producer' }));

  console.log(`\nDemo running: publishing every ${interval} ms. Open http://localhost:8080 and press Ctrl+C to stop.\n`);

  while (true) {
    const event = pick(WEIGHTED);
    const producer = pick(producers);
    console.log(`${producer.name} ▶ ${event.key}`);
    producer.publish(event.key, event.payload());
    await sleep(interval);
  }
}

main().catch((err) => {
  console.error(`✖ Could not connect to broker: ${err.message}. Start it first with: npm start`);
  process.exit(1);
});
