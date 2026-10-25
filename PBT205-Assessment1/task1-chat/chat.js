const amqp = require('amqplib');
const { checkRoomName } = require('./rooms');
const readline = require('readline');

const username = process.argv[2];
const port = process.argv[3] || 5672;
const room = process.argv[4] || 'room';

if (!username) {
  console.log('Usage: node chat.js <username> [port] [room]');
  process.exit(1);
}

const roomError = checkRoomName(room);
if (roomError) {
  console.log(roomError);
  process.exit(1);
}

async function main() {
  const conn = await amqp.connect(`amqp://localhost:${port}`);
  const ch = await conn.createChannel();

  // A fanout exchange sends every message to every listener (like a chat room)
  await ch.assertExchange(room, 'fanout', { durable: false });

  // Each user gets their own temporary queue bound to the room
  const q = await ch.assertQueue('', { exclusive: true });
  await ch.bindQueue(q.queue, room, '');

  // Print messages from other users
  ch.consume(q.queue, (msg) => {
    let data;
    try { data = JSON.parse(msg.content.toString()); } catch { return; }
    if (data.user !== username) {
      console.log(`\n[${data.user}]: ${data.text}`);
    }
  }, { noAck: true });

  console.log(`Joined "${room}" as ${username}. Type a message and press Enter.`);

  // Send what the user types
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    if (!line.trim()) return;
    const message = JSON.stringify({ user: username, text: line });
    ch.publish(room, '', Buffer.from(message));
  });
}

main().catch((err) => {
  console.error('Could not connect to RabbitMQ:', err.message);
  process.exit(1);
});