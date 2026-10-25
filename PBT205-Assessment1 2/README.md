PBT205 Assessment 1: chat, trading and contact tracing over RabbitMQ

Everything is Node.js. `server.js` is one web server that serves the GUIs for all three tasks, and the command line programs run separately.

Running it

1. start rabbitmq: `docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management`
2. `npm install` (once)
3. `node server.js` then go to http://localhost:3000. You can add the rabbitmq port and board size if you want, like `node server.js 5672 20`

In all the commands below `port` is the rabbitmq port, normally 5672.

Task 1, chat

- `node task1-chat/chat.js <username> [port] [room]`
- GUI: http://localhost:3000/chat.html (pick a room or type a new one)

Rooms are fanout exchanges and everyone who joins gets their own temp queue. Room names can be letters, numbers, spaces, - and _ (40 max). You can't use orders, trades, position, query or query-response because those are the other topics.

Task 2, trading

- `node task2-trading/exchange.js [port]` (only run one)
- `node task2-trading/sendOrder.js <username> <port> <BUY|SELL> <quantity> <price> [stock]`
- `node task2-trading/watchTrades.js [port]`
- GUI: http://localhost:3000/trading.html

Topics are `orders` and `trades`. Quantity is always 100. Stock defaults to XYZ, any other name works for the multiple stocks part. Orders only match if the exchange is running.

Task 3, contact tracing

- `node task3-tracing/tracker.js [port] [boardSize]` (only run one)
- `node task3-tracing/person.js <port> <personId> [speed] [boardSize]`
- `node task3-tracing/query.js <port> <personId>`
- GUI: http://localhost:3000/tracing.html

Board size is 1 to 1000 (default 10) and the tracker, the people and server.js all need the same one. Speed is moves per second, like `2`, or slow/medium/fast, or `3s` for one move every 3 seconds (the brief says every n seconds). Topics are `position`, `query` and `query-response`.

Quick try with a few terminals:

```
node task3-tracing/tracker.js 5672 10
node task3-tracing/person.js 5672 Alice 3 10
node task3-tracing/person.js 5672 Bob 3 10
node task3-tracing/query.js 5672 Alice
```

How task 3 works: a person sends `{id, x, y, time}` to `position` when they start and after every move. They move one square like a king in chess, and only pick moves that stay on the board. On Ctrl+C they send `{id, leave: true}` so they come off the board. The tracker keeps everyone's current square and when someone steps onto a square that already has people, it logs a contact for each pair (square and time). Off board or non whole number positions are ignored. A query puts the name on `query` and the tracker replies on `query-response`, newest first, and prints the same list in its own console. The query program waits for the answer to its own request id, prints it and exits (gives up after 5 seconds if there's no tracker).

The GUI (`lib/tracing.js`) listens to `position` to draw the board and uses `query` and `query-response` to ask the tracker. The contact log only lives in the tracker. Click a person to query them, their contact squares go blue and squares with 2 or more people go red. You can also join as a person on the page and move with arrow keys or WASD (Q E Z C for diagonals).

Tests

`npm test` needs rabbitmq running (set AMQP_PORT if it isn't 5672). It starts the real programs and checks the chat, the exchange matching, the tracker contacts and reverse order query, movement on the edges and corners of 1, 2, 10 and 1000 sized boards, the GUI bridges, and that bad input (junk orders, a bad price, room names like trades or amq.direct, websockets from other sites) doesn't crash anything.

Things that don't work or are missing

- everything is in memory so if the tracker or exchange restarts the history and order book are gone
- only run one exchange and one tracker, a second exchange would split the orders
- two command line people with the same name get treated as one person
- the web server has no login, only use it on a network you trust
- contact times are when the tracker got the move, not when the person sent it

Folders

- `server.js`, `lib/` the web server and the bridges to rabbitmq
- `public/` the pages
- `task1-chat/`, `task2-trading/`, `task3-tracing/` the command line programs
- `task3-tracing/conor-original/` the first version of task 3, kept for reference. It used `Join` and `locationupdate` queues instead of the position/query topics from the brief and had no tracker, query program or GUI
- `test/integration.js`
