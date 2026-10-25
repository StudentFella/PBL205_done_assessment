// one server for all 3 tasks, serves the gui pages
// usage: node server.js [rabbitmqPort] [boardSize]
// the cli programs (chat.js, exchange.js, tracker.js etc) still run on their own and use the same rabbitmq
const path = require('path');
const http = require('http');
const express = require('express');
const amqp = require('amqplib');

const chat = require('./lib/chat');
const trading = require('./lib/trading');
const tracing = require('./lib/tracing');
const { parseBoardSize } = require('./task3-tracing/common');

const AMQP_PORT = process.argv[2] || 5672;
const BOARD_SIZE = parseBoardSize(process.argv[3] || process.env.BOARD_SIZE);
const WEB_PORT = process.env.PORT || 3000;

async function main() {
  const conn = await amqp.connect(`amqp://localhost:${AMQP_PORT}`);
  // if rabbitmq dies just stop, no point staying up
  conn.on('error', (err) => { console.error('RabbitMQ connection error:', err.message); process.exit(1); });
  conn.on('close', () => { console.error('RabbitMQ connection closed. Stopping.'); process.exit(1); });

  const app = express();
  app.use(express.static(path.join(__dirname, 'public')));
  const server = http.createServer(app);

  // each gui gets its own websocket path so they can share the one port
  const sockets = {
    '/ws/chat': await chat.init(conn),
    '/ws/trading': await trading.init(conn),
    '/ws/tracing': await tracing.init(conn, { boardSize: BOARD_SIZE })
  };

  server.on('upgrade', (req, socket, head) => {
    const wss = sockets[new URL(req.url, 'http://localhost').pathname];
    if (!wss) { socket.destroy(); return; }
    // only let our own pages open websockets, otherwise any random site in the browser could hit localhost:3000
    const origin = req.headers.origin;
    if (origin && new URL(origin).host !== req.headers.host) { socket.destroy(); return; }
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });

  server.on('error', (err) => {
    console.error(err.code === 'EADDRINUSE'
      ? `Port ${WEB_PORT} is already in use. Is another copy of the server running? (Use PORT=3001 node server.js to pick another port.)`
      : err.message);
    process.exit(1);
  });

  server.listen(WEB_PORT, () => {
    console.log(`PBT205 server running at http://localhost:${WEB_PORT}`);
    console.log(`  RabbitMQ port: ${AMQP_PORT}, contact tracing board: ${BOARD_SIZE}x${BOARD_SIZE}`);
  });
}

main().catch((err) => {
  console.error('Could not start. Is RabbitMQ running?', err.message);
  process.exit(1);
});
