// person for task 3, starts somewhere random and keeps moving
// usage: node person.js <port> <personId> [speed] [boardSize]
//   speed is moves per second (like 2), slow/medium/fast, or 2s for one move every 2 seconds, default is 1
const { TOPICS, parseBoardSize, parseSpeed, connect, publish, randomMove } = require('./common');

const [port, id, speedArg, sizeArg] = process.argv.slice(2);

if (!port || !id) {
  console.log('Usage: node person.js <port> <personId> [speed] [boardSize]');
  console.log('Example: node person.js 5672 Alice 2 10   (or 3s for one move every 3 seconds)');
  process.exit(1);
}

const speed = parseSpeed(speedArg);
if (speed === null) {
  console.log('Speed must be moves per second (up to 100), slow / medium / fast, or a number of seconds like 2s.');
  process.exit(1);
}
const boardSize = parseBoardSize(sizeArg);

async function main() {
  const { conn, ch } = await connect(port);

  let x = Math.floor(Math.random() * boardSize);
  let y = Math.floor(Math.random() * boardSize);
  publish(ch, TOPICS.position, { id, x, y, time: new Date().toISOString() });
  console.log(`${id} started at (${x}, ${y}) on a ${boardSize}x${boardSize} board, ${speed} move(s) per second`);

  // 1000 / speed = ms between moves
  const timer = setInterval(() => {
    [x, y] = randomMove(x, y, boardSize);
    publish(ch, TOPICS.position, { id, x, y, time: new Date().toISOString() });
    console.log(`${id} moved to (${x}, ${y})`);
  }, 1000 / speed);

  // ctrl+c tells the tracker we left so we come off the board
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    clearInterval(timer);
    try {
      publish(ch, TOPICS.position, { id, leave: true, time: new Date().toISOString() });
      await ch.close();
      await conn.close();
    } catch { /* doesn't matter */ }
    process.exit(0);
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}

main().catch((err) => {
  console.error('Could not connect to RabbitMQ:', err.message);
  process.exit(1);
});
