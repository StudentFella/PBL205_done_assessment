// starts the web server, the exchange and the tracker in one go
// usage: node start-all.js [rabbitmqPort] [boardSize]   (or npm run all)
const { spawn } = require('child_process');

const port = process.argv[2] || '5672';
const size = process.argv[3] || '10';

const jobs = [
  ['server ', ['server.js', port, size]],
  ['exchange', ['task2-trading/exchange.js', port]],
  ['tracker ', ['task3-tracing/tracker.js', port, size]]
];

const kids = jobs.map(([name, args]) => {
  const child = spawn(process.execPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  const show = (data) => data.toString().split('\n').filter(Boolean).forEach((l) => console.log(`[${name}] ${l}`));
  child.stdout.on('data', show);
  child.stderr.on('data', show);
  child.on('exit', (code) => {
    console.log(`[${name}] stopped (code ${code}), shutting everything down`);
    stopAll();
  });
  return child;
});

let stopping = false;
function stopAll() {
  if (stopping) return;
  stopping = true;
  kids.forEach((k) => k.kill());
  setTimeout(() => process.exit(0), 300);
}
process.on('SIGINT', stopAll);
process.on('SIGTERM', stopAll);

console.log('Starting server, exchange and tracker. Ctrl+C stops all three.');
console.log('Open http://localhost:3000. People still need person.js, or join on the tracing page.');
