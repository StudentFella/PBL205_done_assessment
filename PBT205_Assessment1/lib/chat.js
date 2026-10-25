// bridge for the task 1 gui
// every browser tab gets its own channel and queue for the room it joins
const { WebSocketServer } = require('ws');
const { checkRoomName } = require('../task1-chat/rooms');

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

async function init(conn) {
  const wss = new WebSocketServer({ noServer: true });

  wss.on('connection', (ws) => {
    let ch = null;
    let user = null;
    let room = null;

    ws.on('message', async (raw) => {
      let data;
      try { data = JSON.parse(raw.toString()); } catch { return; }

      if (data.type === 'join' && !ch) {
        user = String(data.user || '').trim();
        room = String(data.room || '').trim();
        if (!user || !room) {
          send(ws, { type: 'error', text: 'Username and room are required.' });
          return;
        }
        const roomError = checkRoomName(room);
        if (roomError || user.length > 40) {
          send(ws, { type: 'error', text: roomError || 'Usernames can be up to 40 characters.' });
          return;
        }
        try {
          ch = await conn.createChannel();
          ch.on('error', () => {}); // stops a bad room name killing the whole server
          await ch.assertExchange(room, 'fanout', { durable: false });
          const q = await ch.assertQueue('', { exclusive: true });
          await ch.bindQueue(q.queue, room, '');
          ch.consume(q.queue, (msg) => {
            let m;
            try { m = JSON.parse(msg.content.toString()); } catch { return; }
            send(ws, { type: 'message', user: m.user, text: m.text });
          }, { noAck: true });
          send(ws, { type: 'joined', user, room });
        } catch (err) {
          ch = null;
          send(ws, { type: 'error', text: 'Could not join room: ' + err.message });
        }
      } else if (data.type === 'message' && ch) {
        const text = String(data.text || '').trim();
        if (!text) return;
        ch.publish(room, '', Buffer.from(JSON.stringify({ user, text })));
      }
    });

    ws.on('close', () => {
      if (ch) ch.close().catch(() => {});
    });
  });

  return wss;
}

module.exports = { init };
