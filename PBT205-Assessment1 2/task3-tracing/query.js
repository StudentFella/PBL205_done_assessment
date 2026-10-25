// task 3 query, asks the tracker who someone met, prints it and exits
// usage: node query.js <port> <personId>
const { randomUUID } = require('crypto');
const { TOPICS, connect, subscribe, publish } = require('./common');

const [port, id] = process.argv.slice(2);

if (!port || !id) {
  console.log('Usage: node query.js <port> <personId>');
  console.log('Example: node query.js 5672 Alice');
  process.exit(1);
}

const TIMEOUT_MS = 5000;

async function main() {
  const { conn, ch } = await connect(port);
  const requestId = randomUUID(); // so we only pick up the answer to our own question

  const answer = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('No answer from the tracker. Is tracker.js running?')), TIMEOUT_MS);
    subscribe(ch, TOPICS.response, (msg) => {
      if (msg.requestId !== requestId) return;
      clearTimeout(timer);
      resolve(msg);
    }).then(() => {
      // publish after we're listening or a quick answer could get missed
      publish(ch, TOPICS.query, { id, requestId });
    }).catch(reject);
  });

  const res = await answer;

  if (!res.known) {
    console.log(`The tracker does not know anyone called "${id}".`);
  } else if (res.contacts.length === 0) {
    console.log(`${id} has not come into contact with anyone yet.`);
  } else {
    console.log(`${id} has come into contact with (most recent first):`);
    res.contacts.forEach((c) => {
      console.log(`  ${new Date(c.time).toLocaleTimeString()}  ${c.with}  at (${c.x}, ${c.y})`);
    });
  }

  await ch.close();
  await conn.close();
}

main().catch((err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
