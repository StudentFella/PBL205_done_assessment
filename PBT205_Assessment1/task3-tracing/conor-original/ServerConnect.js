
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

class Connection {
    queue_Join = 'Join';
    queue_LocationUpdate = 'locationupdate';
    connection = 'amqp://localhost';

    notificationQueue = `notifications_${randomUUID()}`;
    notificationConnection = null;
    notificationChannel = null;

    // Create a persistent connection for incoming notifications.
    async startNotifications() {
        if (this.notificationChannel) {
            return;
        }

        this.notificationConnection = await amqp.connect(this.connection);

        try {
            this.notificationChannel =
                await this.notificationConnection.createChannel();

            await this.notificationChannel.assertQueue(
                this.notificationQueue,
                {
                    durable: false,
                    exclusive: true,
                    autoDelete: true,
                    arguments: {
                        'x-queue-type': 'classic'
                    }
                }
            );

            await this.notificationChannel.consume(
                this.notificationQueue,
                (message) => {
                    if (!message) return;

                    try {
                        const notification = JSON.parse(
                            message.content.toString()
                        );

                        console.log('\n*** NOTIFICATION ***');
                        console.log(notification.message);
                        console.log(
                            'Position:',
                            notification.User_Position
                        );
                        console.log('********************\n');
                    } catch (error) {
                        console.error(
                            'Unable to read notification:',
                            error.message
                        );
                    }
                },
                { noAck: true }
            );

            console.log('Listening for server notifications...');
        } catch (error) {
            await this.close();
            throw error;
        }
    }

    // Send a request and wait for its matching response.
    async sendRequest(queueName, request, timeoutMs = 5000) {
        const connection = await amqp.connect(this.connection);
        let channel;

        try {
            channel = await connection.createChannel();

            const replyQueueName = `reply_${randomUUID()}`;

            const replyQueue = await channel.assertQueue(
                replyQueueName,
                {
                    durable: false,
                    exclusive: true,
                    autoDelete: true,
                    arguments: {
                        'x-queue-type': 'classic'
                    }
                }
            );

            const correlationId = randomUUID();

            return await new Promise((resolve, reject) => {
                let finished = false;
                let timeout;

                const finish = (error, response) => {
                    if (finished) return;

                    finished = true;
                    clearTimeout(timeout);

                    if (error) {
                        reject(error);
                    } else {
                        resolve(response);
                    }
                };

                timeout = setTimeout(() => {
                    finish(
                        new Error(
                            `No response received from the server for ${queueName}.`
                        )
                    );
                }, timeoutMs);

                channel.consume(
                    replyQueue.queue,
                    (message) => {
                        if (!message || finished) return;

                        if (
                            message.properties.correlationId !==
                            correlationId
                        ) {
                            return;
                        }

                        try {
                            const response = JSON.parse(
                                message.content.toString()
                            );

                            finish(null, response);
                        } catch (error) {
                            finish(error);
                        }
                    },
                    { noAck: true }
                ).then(() => {
                    if (finished) return;

                    channel.sendToQueue(
                        queueName,
                        Buffer.from(JSON.stringify(request)),
                        {
                            persistent: true,
                            replyTo: replyQueue.queue,
                            correlationId
                        }
                    );
                }).catch((error) => {
                    finish(error);
                });
            });
        } finally {
            if (channel) {
                await channel.close().catch(() => {});
            }

            await connection.close().catch(() => {});
        }
    }

    // Join the server and store the assigned user details.
    async joinServer(Person) {
        await this.startNotifications();

        try {
            const request = {
                Username: Person.Username,
                NotificationQueue: this.notificationQueue
            };

            const response = await this.sendRequest(
                this.queue_Join,
                request
            );

            if (response.status !== 'joined') {
                throw new Error(
                    response.message || 'Unable to join the server.'
                );
            }

            Person.User_ID = response.User_ID;
            Person.User_Position = response.User_Position;

            console.log('\nSuccessfully joined the server!');
            console.log('User ID:', Person.User_ID);
            console.log('Position:', Person.User_Position);

            return Person;
        } catch (error) {
            await this.close();
            throw error;
        }
    }

    // Send the proposed location to the server.
    async updateLocation(Person, newPosition) {
        if (!Person.User_ID) {
            throw new Error('You must join the server first.');
        }

        const request = {
            User_ID: Person.User_ID,
            User_Position: newPosition
        };

        const response = await this.sendRequest(
            this.queue_LocationUpdate,
            request
        );

        if (
            response.status === 'updated' ||
            response.status === 'collision'
        ) {
            Person.User_Position = response.User_Position;
        }

        return response;
    }

    // Close the notification connection when leaving.
    async close() {
        const channel = this.notificationChannel;
        const connection = this.notificationConnection;

        this.notificationChannel = null;
        this.notificationConnection = null;

        if (channel) {
            await channel.close().catch(() => {});
        }

        if (connection) {
            await connection.close().catch(() => {});
        }
    }
}

module.exports = Connection;
