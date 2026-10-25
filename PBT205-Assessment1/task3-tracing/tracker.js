// task 3 tracker, keeps track of where everyone is, logs contacts and answers queries
// usage: node tracker.js [port] [boardSize]
const { TOPICS, parseBoardSize, connect, subscribe, publish, inBoard } = require('./common');

const port = process.argv[2] || 5672;
const boardSize = parseBoardSize(process.argv[3]);

const positions = new Map();   // personId -> {x, y}, the view of the environment
const cells = new Map();       // "x,y" -> people on that square, so we only check one square
const history = new Map();     // personId -> [{with, x, y, time}] oldest first, this is the contact log
// todo all in memory, would need a db to survive a restart

const key = (x, y) => `${x},${y}`;

function removeFromCell(id) {
  const pos = positions.get(id);
  if (!pos) return;
  const k = key(pos.x, pos.y);
  const set = cells.get(k);
  if (set) {
    set.delete(id);
    if (set.size === 0) cells.delete(k);
  }
}

function logContact(a, b, x, y, time) {
  if (!history.has(a)) history.set(a, []);
  if (!history.has(b)) history.set(b, []);
  history.get(a).push({ with: b, x, y, time });
  history.get(b).push({ with: a, x, y, time });
  console.log(`CONTACT: ${a} and ${b} at (${x}, ${y})`);
}

// someone sent a position (start or a move)
function handlePosition(msg) {
  const id = typeof msg.id === 'string' ? msg.id.trim() : '';
  if (!id) return;

  // they left, keep their history so they can still be queried
  if (msg.leave) {
    removeFromCell(id);
    positions.delete(id);
    console.log(`${id} left the board`);
    return;
  }

  // ignore anything off the board or not a whole number
  if (!inBoard(msg.x, msg.y, boardSize)) {
    console.log(`Ignored position from ${id}: (${msg.x}, ${msg.y}) is outside the ${boardSize}x${boardSize} board`);
    return;
  }

  const old = positions.get(id);
  if (old && old.x === msg.x && old.y === msg.y) return; // didn't actually move

  removeFromCell(id);
  positions.set(id, { x: msg.x, y: msg.y });
  if (!history.has(id)) history.set(id, []);

  const k = key(msg.x, msg.y);
  if (!cells.has(k)) cells.set(k, new Set());
  const here = cells.get(k);

  // anyone already here counts as a contact
  const time = new Date().toISOString();
  for (const other of here) logContact(id, other, msg.x, msg.y, time);
  here.add(id);
}

// someone asked about a person, reply on query-response
function handleQuery(ch, msg) {
  const id = typeof msg.id === 'string' ? msg.id.trim() : '';
  if (!id) return;

  const known = history.has(id);
  // stored oldest first so flip a copy
  const contacts = known ? [...history.get(id)].reverse() : [];

  console.log(`\nQuery for ${id}: ${known ? contacts.length + ' contact(s)' : 'unknown person'}`);
  if (known && contacts.length === 0) console.log('  (has not met anyone yet)');
  contacts.forEach((c) => {
    console.log(`  ${new Date(c.time).toLocaleTimeString()}  ${c.with}  at (${c.x}, ${c.y})`);
  });

  publish(ch, TOPICS.response, { id, requestId: msg.requestId || null, known, contacts });
}

async function main() {
  const { ch } = await connect(port);
  await subscribe(ch, TOPICS.position, handlePosition);
  await subscribe(ch, TOPICS.query, (msg) => handleQuery(ch, msg));
  console.log(`Tracker running on a ${boardSize}x${boardSize} board. Waiting for people...`);
}

main().catch((err) => {
  console.error('Could not connect to RabbitMQ:', err.message);
  process.exit(1);
});
