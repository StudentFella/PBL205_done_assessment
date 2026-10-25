// bridge for the task 2 gui, shows the latest price and sends orders from the browser
const { WebSocketServer } = require('ws');

async function init(conn) {
  const wss = new WebSocketServer({ noServer: true });
  const latest = {};   // latest trade for each stock
  const history = [];  // last 20 or so, newest first

  const ch = await conn.createChannel();
  ch.on('error', (err) => console.error('Trading channel error:', err.message));
  await ch.assertExchange('orders', 'fanout', { durable: false });
  await ch.assertExchange('trades', 'fanout', { durable: false });

  function broadcast(obj) {
    const s = JSON.stringify(obj);
    wss.clients.forEach((c) => { if (c.readyState === 1) c.send(s); });
  }

  wss.on('connection', (ws) => {
    ws.send(JSON.stringify({ type: 'snapshot', latest, history }));

    ws.on('message', (raw) => {
      let d;
      try { d = JSON.parse(raw.toString()); } catch { return; }
      if (d.type !== 'order') return;

      const side = String(d.side || '').toUpperCase();
      const price = parseFloat(d.price);
      const user = String(d.user || '').trim();
      const stock = String(d.stock || 'XYZ').trim().toUpperCase();
      if (!user || !['BUY', 'SELL'].includes(side) || isNaN(price)) return;

      ch.publish('orders', '', Buffer.from(JSON.stringify({
        user, side, quantity: 100, price, stock
      })));
    });
  });

  const q = await ch.assertQueue('', { exclusive: true });
  await ch.bindQueue(q.queue, 'trades', '');
  ch.consume(q.queue, (msg) => {
    let t;
    try { t = JSON.parse(msg.content.toString()); } catch { return; }
    // skip anything that isn't a real trade
    if (!t || typeof t.stock !== 'string' || typeof t.price !== 'number' || !isFinite(t.price)) return;
    latest[t.stock] = t;
    history.unshift(t);
    if (history.length > 20) history.pop();
    broadcast({ type: 'trade', trade: t });
  }, { noAck: true });

  return wss;
}

module.exports = { init };
