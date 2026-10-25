// rules for room names, used by chat.js and the gui bridge
// a room is a rabbitmq exchange so it can't be called orders, trades, position etc or start with amq.
// a room called amq.direct used to crash the server and one called trades leaked into the trading feed
const RESERVED = ['orders', 'trades', 'position', 'query', 'query-response'];

// gives back an error message, or null if the name is ok
function checkRoomName(room) {
  if (!/^[A-Za-z0-9_ -]{1,40}$/.test(room)) {
    return 'Room names can only use letters, numbers, spaces, - and _ (up to 40 characters).';
  }
  if (RESERVED.includes(room.toLowerCase())) {
    return `"${room}" is used by the trading and tracing prototypes. Pick another room name.`;
  }
  return null;
}

module.exports = { checkRoomName };
