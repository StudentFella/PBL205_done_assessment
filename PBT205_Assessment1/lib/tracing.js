// bridge between the task 3 gui and rabbitmq
// the browser can't talk to rabbitmq so this does it for it (position, query, query-response)
// the contact log is in tracker.js not here
const { randomUUID } = require('crypto');
const { WebSocketServer } = require('ws');
const { TOPICS, connect, subscribe, publish, inBoard } = require('../task3-tracing/common');

const QUERY_TIMEOUT_MS = 4000;

async function init(conn, { boardSize }) {
  const wss = new WebSocketServer({ noServer: true });
  const ch = await conn.createChannel();
  ch.on('error', (err) => console.error('Tracing channel error:', err.message));
  for (const name of Object.values(TOPICS)) await ch.assertExchange(name, 'fanout', { durable: false });

  // todo board size is set when the server starts, would be nicer to change it live
  const people = new Map();     // id -> {x, y}, only used for drawing
  const pending = new Map();    // queries waiting on the tracker

  function broadcast(obj) {
    const s = JSON.stringify(obj);
    wss.clients.forEach((c) => { if (c.readyState === 1) c.send(s); });
  }

  // rabbitmq to browsers
  await subscribe(ch, TOPICS.position, (msg) => {
    const id = typeof msg.id === 'string' ? msg.id.trim() : '';
    if (!id) return;
    if (msg.leave) {
      people.delete(id);
      broadcast({ type: 'left', id });
    } else if (inBoard(msg.x, msg.y, boardSize)) {
      people.set(id, { x: msg.x, y: msg.y });
      broadcast({ type: 'position', id, x: msg.x, y: msg.y });
    }
  });

  await subscribe(ch, TOPICS.response, (msg) => {
    const waiting = pending.get(msg.requestId);
    if (!waiting) return; // not ours, probably the query cli
    clearTimeout(waiting.timer);
    pending.delete(msg.requestId);
    if (waiting.ws.readyState === 1) {
      waiting.ws.send(JSON.stringify({ type: 'result', id: msg.id, known: msg.known, contacts: msg.contacts }));
    }
  });

  // browsers to rabbitmq
  wss.on('connection', (ws) => {
    let player = null; // only set once this browser joins

    ws.send(JSON.stringify({
      type: 'snapshot',
      boardSize,
      people: [...people].map(([id, p]) => ({ id, x: p.x, y: p.y }))
    }));

    ws.on('message', (raw) => {
      let d;
      try { d = JSON.parse(raw.toString()); } catch { return; }

      if (d.type === 'query') {
        const id = String(d.id || '').trim().slice(0, 40);
        if (!id) return;
        const requestId = randomUUID();
        const timer = setTimeout(() => {
          pending.delete(requestId);
          if (ws.readyState === 1) {
            ws.send(JSON.stringify({ type: 'error', text: 'The tracker did not answer. Is tracker.js running?' }));
          }
        }, QUERY_TIMEOUT_MS);
        pending.set(requestId, { ws, timer });
        publish(ch, TOPICS.query, { id, requestId });

      } else if (d.type === 'join' && !player) {
        const id = String(d.id || '').trim().slice(0, 40);
        if (!id) { ws.send(JSON.stringify({ type: 'error', text: 'Enter a name to join.' })); return; }
        if (people.has(id)) { ws.send(JSON.stringify({ type: 'error', text: `"${id}" is already on the board. Names must be unique.` })); return; }
        player = { id, x: Math.floor(Math.random() * boardSize), y: Math.floor(Math.random() * boardSize) };
        publish(ch, TOPICS.position, { id, x: player.x, y: player.y, time: new Date().toISOString() });
        ws.send(JSON.stringify({ type: 'joined', id }));

      } else if (d.type === 'move' && player) {
        // browser sends a direction (dx and dy are -1, 0 or 1), we check it stays on the board here
        const dx = Math.sign(Number(d.dx) || 0);
        const dy = Math.sign(Number(d.dy) || 0);
        const nx = player.x + dx;
        const ny = player.y + dy;
        if ((dx === 0 && dy === 0) || !inBoard(nx, ny, boardSize)) return;
        player.x = nx;
        player.y = ny;
        publish(ch, TOPICS.position, { id: player.id, x: nx, y: ny, time: new Date().toISOString() });

      } else if (d.type === 'leave' && player) {
        publish(ch, TOPICS.position, { id: player.id, leave: true, time: new Date().toISOString() });
        player = null;
      }
    });

    ws.on('close', () => {
      // tab closed so take their person off the board
      if (player) publish(ch, TOPICS.position, { id: player.id, leave: true, time: new Date().toISOString() });
      for (const [rid, w] of pending) {
        if (w.ws === ws) { clearTimeout(w.timer); pending.delete(rid); }
      }
    });
  });

  return wss;
}

module.exports = { init };
