
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

class GameServer {
    queue_Join = 'Join';
    queue_LocationUpdate = 'locationupdate';
    connectionString = 'amqp://localhost';

    users = new Map();
    boardSize = 10;

    async start() {
        // Connect to RabbitMQ.
        this.connection = await amqp.connect(this.connectionString);
        this.channel = await this.connection.createChannel();

        // Create the permanent Join queue.
        await this.channel.assertQueue(this.queue_Join, {
            durable: true,
            arguments: {
                'x-queue-type': 'quorum'
            }
        });

        // Create the permanent location update queue.
        await this.channel.assertQueue(this.queue_LocationUpdate, {
            durable: true,
            arguments: {
                'x-queue-type': 'quorum'
            }
        });

        await this.channel.prefetch(1);

        console.log('Server started.');
        console.log('Board size:', this.boardSize, 'x', this.boardSize);
        console.log('Waiting for clients to join...');
        console.log('Waiting for location updates...');

        // Listen for join requests.
        await this.channel.consume(
            this.queue_Join,
            async (message) => {
                if (!message) return;

                try {
                    const request = JSON.parse(
                        message.content.toString()
                    );

                    const replyTo = message.properties.replyTo;
                    const correlationId =
                        message.properties.correlationId;

                    console.log(
                        'Join request received:',
                        request.Username
                    );

                    if (
                        !replyTo ||
                        !correlationId ||
                        !request.Username ||
                        !request.NotificationQueue
                    ) {
                        if (replyTo && correlationId) {
                            this.channel.sendToQueue(
                                replyTo,
                                Buffer.from(JSON.stringify({
                                    status: 'rejected',
                                    message: 'Join request is missing required information.'
                                })),
                                { correlationId }
                            );
                        }

                        this.channel.ack(message);
                        return;
                    }

                    // Find a free starting square.
                    const position = this.generatePosition();

                    if (position === null) {
                        this.channel.sendToQueue(
                            replyTo,
                            Buffer.from(JSON.stringify({
                                status: 'rejected',
                                message: 'The board is full.'
                            })),
                            { correlationId }
                        );

                        this.channel.ack(message);
                        return;
                    }

                    // Create and store the user.
                    const user = {
                        Username: request.Username,
                        User_ID: randomUUID(),
                        User_Position: position,
                        NotificationQueue: request.NotificationQueue
                    };

                    this.users.set(user.User_ID, user);

                    console.log('User joined:', {
                        Username: user.Username,
                        User_ID: user.User_ID,
                        User_Position: user.User_Position
                    });

                    // Return the user's ID and starting position.
                    const response = {
                        status: 'joined',
                        User_ID: user.User_ID,
                        User_Position: user.User_Position
                    };

                    this.channel.sendToQueue(
                        replyTo,
                        Buffer.from(JSON.stringify(response)),
                        { correlationId }
                    );

                    this.channel.ack(message);
                } catch (error) {
                    console.error(
                        'Error processing join request:',
                        error
                    );

                    this.channel.nack(message, false, false);
                }
            },
            { noAck: false }
        );

        // Listen for location updates.
        await this.channel.consume(
            this.queue_LocationUpdate,
            async (message) => {
                if (!message) return;

                await this.handleLocationUpdate(message);
            },
            { noAck: false }
        );
    }

    // Process a player's proposed new location.
    async handleLocationUpdate(message) {
        const replyTo = message.properties.replyTo;
        const correlationId = message.properties.correlationId;

        try {
            const request = JSON.parse(
                message.content.toString()
            );

            const user = this.users.get(request.User_ID);

            if (!user) {
                this.sendResponse(
                    replyTo,
                    correlationId,
                    {
                        status: 'rejected',
                        message: 'User was not found on the server.'
                    }
                );

                this.channel.ack(message);
                return;
            }

            // Validate the proposed coordinates.
            if (typeof request.User_Position !== 'string') {
                this.sendResponse(
                    replyTo,
                    correlationId,
                    {
                        status: 'rejected',
                        message: 'Invalid location format.'
                    }
                );

                this.channel.ack(message);
                return;
            }

            const coordinates = request.User_Position
                .split(',')
                .map(value => value.trim());

            const [x, y] = coordinates.map(Number);

            if (
                coordinates.length !== 2 ||
                coordinates.some(value => value === '') ||
                !Number.isInteger(x) ||
                !Number.isInteger(y) ||
                x < 0 || x >= this.boardSize ||
                y < 0 || y >= this.boardSize
            ) {
                this.sendResponse(
                    replyTo,
                    correlationId,
                    {
                        status: 'rejected',
                        message: 'That location is outside the board.'
                    }
                );

                this.channel.ack(message);
                return;
            }

            const newPosition = `${x},${y}`;

            // Update the authoritative position stored on the server.
            user.User_Position = newPosition;

            console.log(
                `${user.Username} moved to ${newPosition}`
            );

            // Find and notify every player in the same square.
            const usersAtPosition =
                this.notifyUsersAtPosition(newPosition);

            const otherUsers = usersAtPosition.filter(
                other => other.User_ID !== user.User_ID
            );

            let response;

            if (otherUsers.length > 0) {
                response = {
                    status: 'collision',
                    User_Position: newPosition,
                    message: `You are in the same position as ${otherUsers
                        .map(other => other.Username)
                        .join(', ')}.`
                };

                console.log(
                    `Collision at ${newPosition}:`,
                    usersAtPosition
                        .map(other => other.Username)
                        .join(', ')
                );
            } else {
                response = {
                    status: 'updated',
                    User_Position: newPosition,
                    message: 'Location updated successfully.'
                };
            }

            // Respond to the player who requested the move.
            this.sendResponse(
                replyTo,
                correlationId,
                response
            );

            this.channel.ack(message);
        } catch (error) {
            console.error(
                'Error processing location update:',
                error
            );

            this.channel.nack(message, false, false);
        }
    }

    // Send a response to the client that made the request.
    sendResponse(replyTo, correlationId, response) {
        if (!replyTo || !correlationId) {
            return;
        }

        this.channel.sendToQueue(
            replyTo,
            Buffer.from(JSON.stringify(response)),
            { correlationId }
        );
    }

    // Find everyone sharing a square and notify each client.
    notifyUsersAtPosition(position) {
        const usersAtPosition = [...this.users.values()].filter(
            user => user.User_Position === position
        );

        // A collision requires at least two users.
        if (usersAtPosition.length < 2) {
            return usersAtPosition;
        }

        for (const user of usersAtPosition) {
            const otherUsernames = usersAtPosition
                .filter(other => other.User_ID !== user.User_ID)
                .map(other => other.Username);

            const notification = {
                type: 'collision',
                User_Position: position,
                message: `You are in the same position as ${otherUsernames.join(', ')}.`
            };

            if (user.NotificationQueue) {
                this.channel.sendToQueue(
                    user.NotificationQueue,
                    Buffer.from(JSON.stringify(notification))
                );
            }
        }

        return usersAtPosition;
    }

    // Assign a starting square that no current user occupies.
    generatePosition() {
        const availablePositions = [];

        for (let x = 0; x < this.boardSize; x++) {
            for (let y = 0; y < this.boardSize; y++) {
                const position = `${x},${y}`;

                const occupied = [...this.users.values()].some(
                    user => user.User_Position === position
                );

                if (!occupied) {
                    availablePositions.push(position);
                }
            }
        }

        if (availablePositions.length === 0) {
            return null;
        }

        const index = Math.floor(
            Math.random() * availablePositions.length
        );

        return availablePositions[index];
    }
}

// Start the server.
const server = new GameServer();

server.start().catch(error => {
    console.error('Failed to start server:', error);
});
