// shared stuff for task 3, every program uses the same 3 topics
const amqp = require('amqplib');

// the 3 topics, fanout like room/orders/trades
const TOPICS = {
  position: 'position', // person tells the tracker where they are
  query: 'query', // asks who someone has met
  response: 'query-response' // the tracker's answer
};

const DEFAULT_BOARD = 10;
const MAX_BOARD = 1000;

// board size can be 1 to 1000, anything else goes back to 10
function parseBoardSize(value) {
  if (value === undefined || value === null || value === '') return DEFAULT_BOARD;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 1 || n > MAX_BOARD) {
    console.log(`Board size "${value}" is not valid (1 to ${MAX_BOARD}). Using ${DEFAULT_BOARD}.`);
    return DEFAULT_BOARD;
  }
  return n;
}

// connects and makes sure the topics exist
async function connect(port) {
  const conn = await amqp.connect(`amqp://localhost:${port}`);
  const ch = await conn.createChannel();
  for (const name of Object.values(TOPICS)) {
    await ch.assertExchange(name, 'fanout', { durable: false });
  }
  return { conn, ch };
}

// listens to a topic on its own temp queue and calls handler with each message
async function subscribe(ch, topic, handler) {
  const q = await ch.assertQueue('', { exclusive: true });
  await ch.bindQueue(q.queue, topic, '');
  await ch.consume(q.queue, (msg) => {
    if (!msg) return;
    let data;
    try { data = JSON.parse(msg.content.toString()); } catch { return; } // junk, skip it
    handler(data);
  }, { noAck: true });
}

function publish(ch, topic, obj) {
  ch.publish(topic, '', Buffer.from(JSON.stringify(obj)));
}

// moves like a king in chess, 8 directions
const KING_MOVES = [
  [-1, -1], [0, -1], [1, -1],
  [-1, 0],           [1, 0],
  [-1, 1],  [0, 1],  [1, 1]
];

function inBoard(x, y, size) {
  return Number.isInteger(x) && Number.isInteger(y) && x >= 0 && y >= 0 && x < size && y < size;
}

// only picks moves that stay on the board so nobody walks off the edge
// (1x1 board has nowhere to go so they stay put)
function randomMove(x, y, size) {
  const options = KING_MOVES
    .map(([dx, dy]) => [x + dx, y + dy])
    .filter(([nx, ny]) => inBoard(nx, ny, size));
  if (options.length === 0) return [x, y];
  return options[Math.floor(Math.random() * options.length)];
}

// speed is moves per second, a word, or something like 2s for one move every 2 seconds
const SPEED_WORDS = { slow: 0.5, medium: 1, fast: 5 };
function parseSpeed(value) {
  if (value === undefined) return 1;
  const word = String(value).toLowerCase();
  if (SPEED_WORDS[word]) return SPEED_WORDS[word];
  const every = /^(\d+(\.\d+)?)s$/.exec(word);
  if (every) {
    const secs = Number(every[1]);
    return secs > 0 && secs <= 3600 ? 1 / secs : null;
  }
  const n = Number(value);
  if (!(n > 0) || n > 100) return null;
  return n;
}

module.exports = {
  TOPICS, DEFAULT_BOARD, MAX_BOARD, parseBoardSize, connect, subscribe, publish,
  KING_MOVES, inBoard, randomMove, parseSpeed
};
