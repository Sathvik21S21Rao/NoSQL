const fs = require('fs');
const readline = require('readline');
const MongoOps = require('./MongoOps');
const PostgreSQLOps = require('./PostgreSQLOps');

const outputLog = fs.createWriteStream('testcase.out', { flags: 'w' });

function logOutput(...args) {
    console.log(...args);
    outputLog.write(args.join(' ') + '\n');
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

    const rl = readline.createInterface({
        input: fs.createReadStream('testcase.in'),
        crlfDelay: Infinity,
    });

    for await (const line of rl) {
        if (!line.trim()) continue;

        try {
            const parts = line.trim().split(' ');
            let timestamp;
            let command;

            // Merge commands don't have timestamp
            if (isNaN(parts[0])) {
                command = line.trim();
            } else {
                timestamp = parseInt(parts[0]);
                command = parts.slice(1).join(' ');
            }

            logOutput(`\n[${timestamp ? timestamp : 'No timestamp'}] >> ${command}`);

            switch (true) {
                case command.startsWith('MONGO.SET'):
                    {
                        const match = command.match(/MONGO\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)/);
                        if (match) {
                            const [, studentID, courseID, grade] = match.map(x => x.trim());
                            await mongo.performOperation('studentgrades', 'grade', 'update', { studentID, courseID, grade }, timestamp);
                        }
                    }
                    break;

                case command.startsWith('MONGO.GET'):
                    {
                        const match = command.match(/MONGO\.GET\(([^,]+),([^)]+)\)/);
                        if (match) {
                            const [, studentID, courseID] = match.map(x => x.trim());
                            const result = await mongo.readRecord('studentgrades', studentID, courseID);
                            logOutput(result ? `MONGO.RESULT: ${JSON.stringify(result)}` : 'MONGO.RESULT: Not found');
                        }
                    }
                    break;

                case command.startsWith('POSTGRES.SET'):
                    {
                        const match = command.match(/POSTGRES\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)/);
                        if (match) {
                            const [, studentID, courseID, grade] = match.map(x => x.trim());
                            await postgres.performOperation('studentgrades', 'grade', 'update', { studentID, courseID, grade }, timestamp);
                        }
                    }
                    break;

                case command.startsWith('POSTGRES.GET'):
                    {
                        const match = command.match(/POSTGRES\.GET\(([^,]+),([^)]+)\)/);
                        if (match) {
                            const [, studentID, courseID] = match.map(x => x.trim());
                            const result = await postgres.readRecord('studentgrades', studentID, courseID);
                            logOutput(result ? `POSTGRES.RESULT: ${JSON.stringify(result)}` : 'POSTGRES.RESULT: Not found');
                        }
                    }
                    break;

                case command.startsWith('MONGO.MERGE'):
                    {
                        const match = command.match(/MONGO\.MERGE\(([^)]+)\)/);
                        if (match) {
                            const dbToMerge = match[1].trim();
                            if (dbToMerge === 'POSTGRES') {
                                await mongo.merge('Postgres');
                                logOutput('MONGO.MERGE completed with POSTGRES');
                            } else {
                                logOutput('Unknown merge database:', dbToMerge);
                            }
                        }
                    }
                    break;

                case command.startsWith('POSTGRES.MERGE'):
                    {
                        const match = command.match(/POSTGRES\.MERGE\(([^)]+)\)/);
                        if (match) {
                            const dbToMerge = match[1].trim();
                            if (dbToMerge === 'MONGO') {
                                console.log('Merge operation here here');
                                await postgres.merge('MongoDB');
                                logOutput('POSTGRES.MERGE completed with MONGO');
                            } else {
                                logOutput('Unknown merge database:', dbToMerge);
                            }
                        }
                    }
                    break;

                default:
                    logOutput('Unknown command:', command);
                    break;
            }
        } catch (err) {
            logOutput('Error processing line:', err);
        }
    }

    await mongo.close();
    await postgres.close();
    outputLog.end();
}

main();