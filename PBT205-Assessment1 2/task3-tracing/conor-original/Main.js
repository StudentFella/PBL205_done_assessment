

const rlSync = require('readline-sync');
const User = require('./User.js');
const Connection = require('./ServerConnect.js');

async function main() {

    const person = new User();
    const server = new Connection();

    // Ask for a username
    person.Username = rlSync.question(
        'Enter a username to join! '
    );

    // Join the server
    try {
        await server.joinServer(person);
    }
    catch (error) {
        console.error('Unable to join server:', error.message);
        return;
    }

    let running = true;

    // allow user to move

    while (running) {

        console.log('\n--- Game Menu ---');
        console.log('W - Move up');
        console.log('S - Move down');
        console.log('A - Move left');
        console.log('D - Move right');
        console.log('P - Show current position');
        console.log('Q - Quit');

        const choice = rlSync.question(
            'Choose an option: '
        ).trim().toUpperCase();

        try {
            switch (choice) {

                case 'W':
                case 'A':
                case 'S':
                case 'D':
                    await person.UserMove(choice, server);
                    break;

                case 'P':
                    console.log(
                        'Username:', person.Username,
                        '\nUser ID:', person.User_ID,
                        '\nPosition:', person.User_Position
                    );
                    break;

                case 'Q':
                    console.log('Leaving the game.');
                    running = false;
                    break;

                default:
                    console.log('Invalid option. Try again.');
                    break;
            }
        }
        catch (error) {
            console.error('Operation failed:', error.message);
        }
    }
}

main().catch(console.error);
