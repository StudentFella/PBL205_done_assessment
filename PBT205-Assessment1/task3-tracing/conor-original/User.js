
class User {

    Username;
    User_ID;
    User_Position;


    async UserMove(direction, server) {
        const moves = {
            W: { x: 0, y: -1 },
            S: { x: 0, y: 1 },
            A: { x: -1, y: 0 },
            D: { x: 1, y: 0 }
        };

        const move = moves[direction.toUpperCase()];

        if (!move) {
            console.log('Invalid direction.');
            return;
        }

        if (!this.User_ID || !this.User_Position) {
            console.log('You must join the server first.');
            return;
        }

        const [x, y] = this.User_Position.split(',').map(Number);

        const newX = x + move.x;
        const newY = y + move.y;

        // Check the board boundaries before sending the update.
        if (newX < 0 || newX >= 10 || newY < 0 || newY >= 10) {
            console.log('You cannot move outside the board.');
            return;
        }

        const newPosition = `${newX},${newY}`;

        const response = await server.updateLocation(this, newPosition);

        if (response.status === 'updated') {
            console.log(response.message);
            console.log('New position:', this.User_Position);
        } else if (response.status === 'collision') {
            console.log(response.message);
            console.log('Your position:', this.User_Position);
        } else {
            console.log(response.message || 'Move was unsuccessful.');
        }
    }


}

module.exports = User;
