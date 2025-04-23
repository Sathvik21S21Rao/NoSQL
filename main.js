const readline = require('readline');
const MongoOps = require('./MongoOps');
const PostgreSQLOps = require('./PostgreSQLOps');
// Assuming PigOps.js is implemented similarly
// const PigOps = require('./PigOps');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

function ask(question) {
    return new Promise(resolve => rl.question(question, resolve));
}

async function main() {
    const mongo = new MongoOps('mongodb://localhost:27017/StudentGrades');
    await mongo.connect();
    const postgres = new PostgreSQLOps({
        host: '127.0.0.1',
        port: 5432,
        user: 'vishruthvijay',
        password: '',
        database: 'studentgrades',
    });

    // const pig = new PigOps(); // Assuming you have implemented similar class for Pig

    try {
        while (true) {
            const action = await ask('\nChoose operation:\n1. Update Record\n2. Merge Ops\n3. Read Record\n4. Exit\n> ');

            if (action === '1') {
                const dbType = await ask('Enter database (MongoDB/Postgres/Pig): ');
                const studentID = await ask('Enter student ID: ');
                const courseID = await ask('Enter course ID: ');
                const grade = await ask('Enter grade: ');
                const data = { studentID, courseID, grade };

                const fieldName = 'grade';
                const operation = 'update';
                const tableOrCollection = 'studentgrades'; // Assuming same name for simplicity

                switch (dbType) {
                    case 'MongoDB':
                        await mongo.performOperation(tableOrCollection, fieldName, operation, data);
                        break;
                    case 'Postgres':
                        await postgres.performOperation(tableOrCollection, fieldName, operation, data);
                        break;
                    case 'Pig':
                        // await pig.performOperation(tableOrCollection, fieldName, operation, data);
                        break;
                    default:
                        console.log('Invalid database selected');
                }
            }

            else if (action === '2') {
                const target = await ask('Merge into which DB? (MongoDB/Postgres/Pig): ');
                const source = await ask('Merge operations from which DB? (MongoDB/Postgres/Pig): ');

                if (target === source) {
                    console.log("Source and target DBs must be different.");
                    return;
                }

                switch (target) {
                    case 'MongoDB':
                        await mongo.merge(source);
                        break;
                    case 'Postgres':
                        await postgres.merge(source);
                        break;
                    case 'Pig':
                        // await pig.merge(source);
                        break;
                    default:
                        console.log("Unknown target DB");
                }
            }

            else if (action === '3') {
                const dbType = await ask('Enter database (MongoDB/Postgres/Pig): ');
                const studentID = await ask('Enter student ID: ');
                const courseID = await ask('Enter course ID: ');

                switch (dbType) {
                    case 'MongoDB':
                        await mongo.readRecord('studentgrades', studentID, courseID);
                        break;
                    case 'Postgres':
                        await postgres.readRecord('studentgrades', studentID, courseID);
                        break;
                    case 'Pig':
                        // await pig.readRecord('studentgrades', studentID, courseID);
                        break;
                    default:
                        console.log('Invalid database selected');
                }
            }

            else if (action === '4') {
                console.log('Exiting...');
                break;
            }

            else {
                console.log('Invalid option!');
            }
        }

    } catch (err) {
        console.error('Error during execution:', err);
    } finally {
        await mongo.close();
        await postgres.close();
        // await pig.close();
        rl.close();
    }
}

main();
