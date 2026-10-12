#!/usr/bin/env node
/**
 * Consumer CLI.
 *
 *   node clients/consumer.js <queue> <bindingKey> [moreKeys...] [--name billing-1]
 *
 * Examples:
 *   node clients/consumer.js billing order.created order.paid
 *   node clients/consumer.js billing order.created --name billing-2   (shares the work round-robin)
 *   node clients/consumer.js email user.registered
 */

const { connect } = require('./client');

const args = process.argv.slice(2);
const nameFlag = args.indexOf('--name');
const customName = nameFlag !== -1 ? args.splice(nameFlag, 2)[1] : null;
const [queue, ...bindingKeys] = args;

const dim = (s) => `\x1b[2m${s}\x1b[0m`;
const green = (s) => `\x1b[32m${s}\x1b[0m`;
const cyan = (s) => `\x1b[36m${s}\x1b[0m`;
const magenta = (s) => `\x1b[35m${s}\x1b[0m`;
const red = (s) => `\x1b[31m${s}\x1b[0m`;

if (!queue) {
  console.log('Usage: node clients/consumer.js <queue> <bindingKey> [moreKeys...] [--name <name>]');
  process.exit(1);
}

async function main() {
  const name = customName || `${queue}-${process.pid}`;
  const client = await connect({ name, role: 'consumer' });

  client.on('brokerError', (msg) => console.error(red(`✖ ${msg.error}`)));
  client.on('subscribed', () => {
    const keys = bindingKeys.length ? bindingKeys.map(magenta).join(', ') : dim('(existing bindings)');
    console.log(`${green(client.name)} listening on queue ${cyan(queue)} ← ${keys}\n`);
  });

  let count = 0;
  client.on('message', ({ queue: q, message }) => {
    count++;
    const time = dim(new Date(message.timestamp).toLocaleTimeString());
    const payload = typeof message.payload === 'string' ? message.payload : JSON.stringify(message.payload);
    console.log(
      `${time}  #${count} ${cyan(q)} ← ${magenta(message.routingKey)}  ${payload}  ${dim(`from ${message.publishedBy}`)}`
    );
  });

  client.on('close', () => {
    console.log(red('\nBroker closed the connection.'));
    process.exit(1);
  });

  client.subscribe(queue, bindingKeys);
}

main().catch((err) => {
  console.error(red(`✖ Could not connect to broker: ${err.message}. Is it running? (npm start)`));
  process.exit(1);
});
