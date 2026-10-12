#!/usr/bin/env node
/**
 * Producer CLI.
 *
 *   node clients/producer.js <routingKey> [payload] [--name web-shop]   send one message and exit
 *   node clients/producer.js [--name web-shop]                          interactive mode
 *
 * In interactive mode, type:  <routingKey> <payload>
 *   e.g.  order.created {"orderId": 7, "total": 120000}
 */

const readline = require('node:readline');
const { connect } = require('./client');
const { parsePayload } = require('../src/protocol');

const args = process.argv.slice(2);
const nameFlag = args.indexOf('--name');
const name = nameFlag !== -1 ? args.splice(nameFlag, 2)[1] : `producer-${process.pid}`;
const [routingKey, ...payloadParts] = args;

const dim = (s) => `\x1b[2m${s}\x1b[0m`;
const green = (s) => `\x1b[32m${s}\x1b[0m`;
const yellow = (s) => `\x1b[33m${s}\x1b[0m`;
const red = (s) => `\x1b[31m${s}\x1b[0m`;

function describe({ id, routedTo }) {
  return routedTo.length
    ? green(`✔ ${id} routed to: ${routedTo.join(', ')}`)
    : yellow(`⚠ ${id} unroutable: no queue is bound to that key, message dropped`);
}

async function main() {
  const client = await connect({ name, role: 'producer' });
  client.on('brokerError', (msg) => console.error(red(`✖ ${msg.error}`)));

  // ── One-shot mode ──
  if (routingKey) {
    client.once('published', (result) => {
      console.log(describe(result));
      client.close();
    });
    client.once('brokerError', () => client.close());
    client.publish(routingKey, parsePayload(payloadParts.join(' ')));
    return;
  }

  // ── Interactive mode ──
  console.log(`Connected as ${green(client.name)}. Type ${dim('<routingKey> <payload>')}, Ctrl+C to quit.\n`);
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: 'kurir> ' });

  client.on('published', (result) => {
    console.log(describe(result));
    rl.prompt();
  });
  client.on('close', () => {
    console.log(red('\nBroker closed the connection.'));
    process.exit(1);
  });

  rl.on('line', (line) => {
    const trimmed = line.trim();
    if (!trimmed) return rl.prompt();
    const space = trimmed.indexOf(' ');
    const key = space === -1 ? trimmed : trimmed.slice(0, space);
    const payload = space === -1 ? '' : trimmed.slice(space + 1);
    client.publish(key, parsePayload(payload));
  });
  rl.on('close', () => {
    client.close();
    process.exit(0);
  });
  rl.prompt();
}

main().catch((err) => {
  console.error(red(`✖ Could not connect to broker: ${err.message}. Is it running? (npm start)`));
  process.exit(1);
});
