// integration tests, needs rabbitmq running (port 5672 or set AMQP_PORT)
// run with npm test
// starts the real programs and checks chat, trading, tracker/person/query and the gui bridge
const assert = require('assert');
const path = require('path');
const { spawn } = require('child_process');
const amqp = require('amqplib');
const WebSocket = require('ws');
const common = require('../task3-tracing/common');

const PORT = process.env.AMQP_PORT || 5672;
const WEB_PORT = 3100;
const root = path.join(__dirname, '..');
const children = [];
let passed = 0;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// starts a node program and collects what it prints
function start(file, args = []) {
  const p = spawn('node', [path.join(root, file), ...args], { stdio: ['pipe', 'pipe', 'pipe'] });
  p.out = '';
  p.stdout.on('data', (d) => { p.out += d; });
  p.stderr.on('data', (d) => { p.out += d; });
  children.push(p);
  return p;
}

// runs a program until it exits and gives back what it printed
function run(file, args = []) {
  return new Promise((resolve) => {
    const p = start(file, args);
    p.on('close', (code) => resolve({ code, out: p.out }));
  });
}

async function waitFor(fn, what, ms = 5000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (fn()) return;
    await sleep(25);
  }
  throw new Error('Timed out waiting for ' + what);
}

async function test(name, fn) {
  try {
    await fn();
    passed++;
    console.log('  PASS', name);
  } catch (err) {
    console.log('  FAIL', name, '\n      ', err.message);
    cleanup();
    process.exit(1);
  }
}

function cleanup() {
  children.forEach((c) => { try { c.kill('SIGKILL'); } catch { /* already dead */ } });
}

async function main() {
  const conn = await amqp.connect(`amqp://localhost:${PORT}`);
  const ch = await conn.createChannel();
  await ch.deleteQueue('exchange_orders'); // old orders left from a previous run would mess with the trading tests

  // task 1
  console.log('Task 1: chat');
  await test('a message from one user reaches another user', async () => {
    const room = 'testroom' + Date.now();
    const a = start('task1-chat/chat.js', ['Alice', PORT, room]);
    const b = start('task1-chat/chat.js', ['Bob', PORT, room]);
    await waitFor(() => a.out.includes('Joined') && b.out.includes('Joined'), 'both users to join');
    a.stdin.write('hello bob\n');
    await waitFor(() => b.out.includes('[Alice]: hello bob'), 'Bob to receive the message');
    assert(!a.out.includes('[Alice]: hello bob'), 'sender should not see their own message echoed');
  });

  await test('rooms are separate', async () => {
    const r1 = 'roomA' + Date.now(), r2 = 'roomB' + Date.now();
    const a = start('task1-chat/chat.js', ['Ann', PORT, r1]);
    const b = start('task1-chat/chat.js', ['Ben', PORT, r2]);
    await waitFor(() => a.out.includes('Joined') && b.out.includes('Joined'), 'join');
    a.stdin.write('private\n');
    await sleep(400);
    assert(!b.out.includes('private'), 'message leaked into another room');
  });

  // task 2
  console.log('Task 2: trading');
  await test('exchange matches BUY and SELL and publishes a trade', async () => {
    await ch.assertExchange('trades', 'fanout', { durable: false });
    const q = await ch.assertQueue('', { exclusive: true });
    await ch.bindQueue(q.queue, 'trades', '');
    const trades = [];
    await ch.consume(q.queue, (m) => { try { trades.push(JSON.parse(m.content.toString())); } catch { /* later tests send junk on purpose */ } }, { noAck: true });

    const ex = start('task2-trading/exchange.js', [PORT]);
    await waitFor(() => ex.out.includes('Exchange running'), 'exchange to start');
    await run('task2-trading/sendOrder.js', ['Alice', PORT, 'BUY', '100', '10.50']);
    await sleep(200);
    assert.strictEqual(trades.length, 0, 'no trade expected with only one order');
    await run('task2-trading/sendOrder.js', ['Bob', PORT, 'SELL', '100', '10.00']);
    await waitFor(() => trades.length === 1, 'a trade');
    assert.strictEqual(trades[0].buyer, 'Alice');
    assert.strictEqual(trades[0].seller, 'Bob');
    assert.strictEqual(trades[0].price, 10.5);
    assert.strictEqual(trades[0].quantity, 100);
  });

  await test('prices that do not cross do not trade', async () => {
    // still using the exchange from the last test
    await run('task2-trading/sendOrder.js', ['Cat', PORT, 'BUY', '100', '5.00', 'ABC']);
    await run('task2-trading/sendOrder.js', ['Dan', PORT, 'SELL', '100', '9.00', 'ABC']);
    await sleep(400);
    const ex = children.find((c) => c.out.includes('Exchange running'));
    assert(ex.out.includes('Added to ABC book'), 'orders should rest in the ABC book');
    assert(!ex.out.includes('TRADE: ABC'), 'should not have traded');
  });

  // task 3
  console.log('Task 3: contact tracing');

  await test('person movement stays on the board and is one king step', () => {
    for (const size of [1, 2, 10, 1000]) {
      for (const [sx, sy] of [[0, 0], [size - 1, size - 1], [0, size - 1], [size - 1, 0], [Math.floor(size / 2), 0]]) {
        for (let i = 0; i < 2000; i++) {
          const [nx, ny] = common.randomMove(sx, sy, size);
          assert(common.inBoard(nx, ny, size), `moved off a ${size}x${size} board from (${sx},${sy}) to (${nx},${ny})`);
          const step = Math.max(Math.abs(nx - sx), Math.abs(ny - sy));
          assert(step === 1 || size === 1, 'must move exactly one square');
        }
      }
    }
  });

  await test('board size and speed arguments are validated', () => {
    assert.strictEqual(common.parseBoardSize('1000'), 1000);
    assert.strictEqual(common.parseBoardSize('1001'), 10);
    assert.strictEqual(common.parseBoardSize('0'), 10);
    assert.strictEqual(common.parseBoardSize('abc'), 10);
    assert.strictEqual(common.parseBoardSize(undefined), 10);
    assert.strictEqual(common.parseSpeed('fast'), 5);
    assert.strictEqual(common.parseSpeed('2.5'), 2.5);
    assert.strictEqual(common.parseSpeed('-1'), null);
    assert.strictEqual(common.parseSpeed('2s'), 0.5);
    assert.strictEqual(common.parseSpeed('0.5s'), 2);
    assert.strictEqual(common.parseSpeed('0s'), null);
  });

  const tracker = start('task3-tracing/tracker.js', [PORT, '10']);
  await waitFor(() => tracker.out.includes('Tracker running'), 'tracker to start');
  const pub = (id, x, y) => ch.publish('position', '', Buffer.from(JSON.stringify({ id, x, y })));
  const step = () => sleep(40);

  // alice at 2,2, bob and carol walk onto it, alice moves to 3,3, dave walks onto that
  pub('Alice', 2, 2); await step();
  pub('Bob', 5, 5); await step();
  pub('Bob', 2, 2); await step();
  pub('Carol', 2, 2); await step();
  pub('Alice', 3, 3); await step();
  pub('Dave', 10, 10); await step();   // off the board, 10x10 is 0 to 9
  pub('Dave', -1, 4); await step();    // also off the board
  pub('Dave', 3, 3); await step();
  pub('Eve', 9, 9); await step();      // meets nobody
  await sleep(200);

  await test('tracker logs contacts when people share a square', () => {
    assert(tracker.out.includes('CONTACT: Bob and Alice at (2, 2)'));
    assert(tracker.out.includes('CONTACT: Carol and Alice at (2, 2)'));
    assert(tracker.out.includes('CONTACT: Carol and Bob at (2, 2)'));
    assert(tracker.out.includes('CONTACT: Dave and Alice at (3, 3)'));
  });

  await test('off-board positions are ignored', () => {
    assert(tracker.out.includes('outside the 10x10 board'));
    assert(!tracker.out.includes('at (10, 10)'));
  });

  await test('query lists contacts in reverse-chronological order', async () => {
    const r = await run('task3-tracing/query.js', [PORT, 'Alice']);
    assert.strictEqual(r.code, 0);
    const order = ['Dave', 'Carol', 'Bob'].map((n) => r.out.indexOf('  ' + n) );
    assert(order.every((i) => i > -1), 'missing a contact:\n' + r.out);
    assert(order[0] < order[1] && order[1] < order[2], 'wrong order:\n' + r.out);
    assert(r.out.includes('at (3, 3)') && r.out.includes('at (2, 2)'));
  });

  await test('query for a person with no contacts', async () => {
    const r = await run('task3-tracing/query.js', [PORT, 'Eve']);
    assert(r.out.includes('has not come into contact with anyone yet'), r.out);
  });

  await test('query for an unknown person', async () => {
    const r = await run('task3-tracing/query.js', [PORT, 'Nobody']);
    assert(r.out.includes('does not know anyone called "Nobody"'), r.out);
  });

  await test('moving away does not create a contact, leaving keeps history', async () => {
    const countContacts = async (who) => (await run('task3-tracing/query.js', [PORT, who])).out.split('\n').filter((l) => /\(\d+, \d+\)/.test(l)).length;
    const before = await countContacts('Bob');
    pub('Bob', 8, 8); await step();
    pub('Bob', 8, 9); await step();
    assert.strictEqual(await countContacts('Bob'), before, 'moving to empty squares must not add contacts');
    ch.publish('position', '', Buffer.from(JSON.stringify({ id: 'Carol', leave: true }))); await step();
    const r = await run('task3-tracing/query.js', [PORT, 'Carol']);
    assert(r.out.includes('Bob') && r.out.includes('Alice'), 'Carol should still be queryable after leaving:\n' + r.out);
  });

  await test('person.js publishes a start position and moves', async () => {
    const seen = [];
    const q = await ch.assertQueue('', { exclusive: true });
    await ch.bindQueue(q.queue, 'position', '');
    await ch.consume(q.queue, (m) => { const d = JSON.parse(m.content.toString()); if (d.id === 'Walker') seen.push(d); }, { noAck: true });
    const p = start('task3-tracing/person.js', [PORT, 'Walker', '20', '3']);
    await waitFor(() => seen.length >= 10, 'Walker to publish 10 positions');
    p.kill('SIGINT');
    await waitFor(() => seen.some((d) => d.leave), 'Walker to publish a leave message');
    const moves = seen.filter((d) => !d.leave);
    moves.forEach((d) => assert(common.inBoard(d.x, d.y, 3), `Walker left the 3x3 board: ${d.x},${d.y}`));
    for (let i = 1; i < moves.length; i++) {
      const s = Math.max(Math.abs(moves[i].x - moves[i - 1].x), Math.abs(moves[i].y - moves[i - 1].y));
      assert.strictEqual(s, 1, 'each move should be exactly one square');
    }
  });

  // gui
  console.log('Web server (all three GUIs)');
  const server = start('server.js', [PORT, '10']);
  await waitFor(() => server.out.includes('running at'), 'server to start');

  await test('landing page and GUI pages are served', async () => {
    for (const p of ['/', '/chat.html', '/trading.html', '/tracing.html']) {
      const res = await fetch(`http://localhost:${WEB_PORT}${p}`).catch(() => null);
      assert(res && res.ok, `${p} not served`);
    }
  });

  const open = (p) => new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${WEB_PORT}${p}`);
    ws.msgs = [];
    ws.on('message', (d) => ws.msgs.push(JSON.parse(d.toString())));
    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
  });

  await test('tracing GUI: join, move and query through the tracker', async () => {
    const ws = await open('/ws/tracing');
    await waitFor(() => ws.msgs.some((m) => m.type === 'snapshot'), 'snapshot');
    assert.strictEqual(ws.msgs[0].boardSize, 10);
    ws.send(JSON.stringify({ type: 'join', id: 'Gui1' }));
    await waitFor(() => ws.msgs.some((m) => m.type === 'joined'), 'joined');
    await waitFor(() => ws.msgs.some((m) => m.type === 'position' && m.id === 'Gui1'), 'own position');
    ws.send(JSON.stringify({ type: 'query', id: 'Alice' }));
    await waitFor(() => ws.msgs.some((m) => m.type === 'result'), 'query result');
    const res = ws.msgs.find((m) => m.type === 'result');
    assert.strictEqual(res.id, 'Alice');
    assert.strictEqual(res.contacts[0].with, 'Dave');
    ws.close();
  });

  await test('tracing GUI: duplicate names are refused and the board edge is respected', async () => {
    const first = await open('/ws/tracing');
    first.send(JSON.stringify({ type: 'join', id: 'Dup1' }));
    await waitFor(() => first.msgs.some((m) => m.type === 'joined'), 'first join');
    const ws = await open('/ws/tracing');
    await waitFor(() => ws.msgs.some((m) => m.type === 'snapshot'), 'snapshot');
    ws.send(JSON.stringify({ type: 'join', id: 'Dup1' }));
    await waitFor(() => ws.msgs.some((m) => m.type === 'error'), 'error');
    first.close();
    ws.send(JSON.stringify({ type: 'join', id: 'Edge' }));
    await waitFor(() => ws.msgs.some((m) => m.type === 'joined'), 'joined');
    for (let i = 0; i < 30; i++) ws.send(JSON.stringify({ type: 'move', dx: -1, dy: -1 }));
    await sleep(500);
    const last = ws.msgs.filter((m) => m.type === 'position' && m.id === 'Edge').pop();
    assert.strictEqual(last.x >= 0 && last.y >= 0, true);
    ws.close();
  });

  await test('chat GUI: two browsers in one room', async () => {
    const a = await open('/ws/chat'), b = await open('/ws/chat');
    const room = 'guiroom' + Date.now();
    a.send(JSON.stringify({ type: 'join', user: 'A', room }));
    b.send(JSON.stringify({ type: 'join', user: 'B', room }));
    await waitFor(() => a.msgs.some((m) => m.type === 'joined') && b.msgs.some((m) => m.type === 'joined'), 'both joined');
    a.send(JSON.stringify({ type: 'message', text: 'hi from A' }));
    await waitFor(() => b.msgs.some((m) => m.type === 'message' && m.text === 'hi from A' && m.user === 'A'), 'B to get message');
    a.close(); b.close();
  });

  await test('trading GUI: order from the browser trades against the exchange', async () => {
    const ws = await open('/ws/trading');
    await waitFor(() => ws.msgs.some((m) => m.type === 'snapshot'), 'snapshot');
    ws.send(JSON.stringify({ type: 'order', user: 'Web1', stock: 'XYZ', side: 'SELL', price: '12.00' }));
    await sleep(150);
    ws.send(JSON.stringify({ type: 'order', user: 'Web2', stock: 'XYZ', side: 'BUY', price: '12.50' }));
    await waitFor(() => ws.msgs.some((m) => m.type === 'trade' && m.trade.price === 12), 'trade at 12');
    ws.close();
  });


  console.log('Bad input must not crash anything');
  await test('exchange ignores garbage and invalid orders and keeps working', async () => {
    const ex = children.find((c) => c.out.includes('Exchange running'));
    ch.publish('orders', '', Buffer.from('not json'));
    ch.publish('orders', '', Buffer.from(JSON.stringify({ user: 'X', side: 'BUY', quantity: 100, price: null })));
    ch.publish('orders', '', Buffer.from(JSON.stringify({ user: 'X', side: 'HOLD', quantity: 100, price: 5 })));
    await sleep(400);
    assert.strictEqual(ex.exitCode, null, 'exchange crashed');
    assert(ex.out.includes('Ignored an order that was not valid JSON'));
    assert((ex.out.match(/Ignored an invalid order/g) || []).length === 2);
  });

  await test('sendOrder rejects a bad price', async () => {
    for (const bad of ['abc', '-5', '0']) {
      const r = await run('task2-trading/sendOrder.js', ['Zed', PORT, 'BUY', '100', bad]);
      assert.strictEqual(r.code, 1, `price ${bad} should be refused`);
    }
  });

  await test('web server survives garbage on the trades topic', async () => {
    ch.publish('trades', '', Buffer.from('not json'));
    ch.publish('trades', '', Buffer.from(JSON.stringify({ hello: 'world' })));
    await sleep(400);
    assert.strictEqual(server.exitCode, null, 'server crashed');
    const ws = await open('/ws/trading');
    await waitFor(() => ws.msgs.some((m) => m.type === 'snapshot'), 'snapshot');
    assert(!Object.keys(ws.msgs[0].latest).includes('undefined'));
    ws.close();
  });

  await test('chat refuses room names that clash with other topics or RabbitMQ', async () => {
    const ws = await open('/ws/chat');
    for (const room of ['amq.direct', 'trades', 'Orders', 'position', 'a/b']) {
      ws.msgs.length = 0;
      ws.send(JSON.stringify({ type: 'join', user: 'u', room }));
      await waitFor(() => ws.msgs.length > 0, 'a reply for ' + room);
      assert.strictEqual(ws.msgs[0].type, 'error', `room "${room}" should be refused`);
    }
    assert.strictEqual(server.exitCode, null, 'server crashed');
    const cli = await run('task1-chat/chat.js', ['u', PORT, 'amq.direct']);
    assert.strictEqual(cli.code, 1);
    ws.close();
  });

  await test('WebSocket from another website is refused', async () => {
    const result = await new Promise((resolve) => {
      const ws = new WebSocket(`ws://localhost:${WEB_PORT}/ws/trading`, { headers: { Origin: 'http://evil.example' } });
      ws.on('open', () => resolve('accepted'));
      ws.on('error', () => resolve('refused'));
      ws.on('close', () => resolve('refused'));
    });
    assert.strictEqual(result, 'refused');
  });

  await test('closing the browser removes its person from the board', async () => {
    const a = await open('/ws/tracing');
    a.send(JSON.stringify({ type: 'join', id: 'Ghost' }));
    await waitFor(() => a.msgs.some((m) => m.type === 'joined'), 'joined');
    a.close();
    await sleep(500);
    const b = await open('/ws/tracing');
    await waitFor(() => b.msgs.some((m) => m.type === 'snapshot'), 'snapshot');
    assert(!b.msgs[0].people.some((p) => p.id === 'Ghost'));
    b.close();
  });

  console.log(`\nAll ${passed} checks passed.`);
  cleanup();
  await conn.close();
  process.exit(0);
}

process.on('exit', cleanup); // kill the servers even if a test crashes
process.env.PORT = String(WEB_PORT);
main().catch((err) => { console.error(err); cleanup(); process.exit(1); });
