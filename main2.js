const fs = require('fs');
const readline = require('readline');
const MongoOps = require('./MongoOps');
const PostgreSQLOps = require('./PostgreSQLOps');
const PigOps = require('./PigOps');

const outputLog = fs.createWriteStream('testcase.out', { flags: 'w' });

function logOutput(...args) {
    console.log(...args);
    outputLog.write(args.join(' ') + '\n');
}

async function main() {

    const postgres = new PostgreSQLOps({
        host: '127.0.0.1',
        port: 5432,
        user: 'postgres',
        password: 'sid194',
        database: 'studentgrades',
    });

    const mongo = new MongoOps('mongodb://localhost:27017/StudentGrades');
    await mongo.connect();
    
    const pig = new PigOps('/user/hadoop1/studentgrades');
    await pig.initializePigOps();

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

                case command.startsWith('PIG.SET'):
                    {
                        const match = command.match(/PIG\.SET\(\(([^,]+),([^)]+)\),\s*([^)]+)\)/);
                        if (match) {
                            const [, studentID, courseID, grade] = match.map(x => x.trim());
                            await pig.performOperation('studentgrades', 'grade', 'update', { studentID:studentID, courseID:courseID, grade:grade }, timestamp);
                        }
                    }
                    break;

                case command.startsWith('PIG.GET'):
                    {
                        const match = command.match(/PIG\.GET\(([^,]+),([^)]+)\)/);
                        if (match) {
                            const [, studentID, courseID] = match.map(x => x.trim());
                            await pig.readRecord('studentgrades', studentID, courseID);
                            logOutput(`PIG.RESULT: Check console output (Pig DUMP result)`);
                        }
                    }
                    break;

                case command.startsWith('MONGO.MERGE'):
                    {
                        const match = command.match(/MONGO\.MERGE\(([^)]+)\)/);
                        if (match) {
                            const dbToMerge = match[1].trim();
                            await mongo.merge(dbToMerge);
                            logOutput(`MONGO.MERGE completed with ${dbToMerge}`);
                        }
                    }
                    break;

                case command.startsWith('POSTGRES.MERGE'):
                    {
                        const match = command.match(/POSTGRES\.MERGE\(([^)]+)\)/);
                        if (match) {
                            const dbToMerge = match[1].trim();
                            await postgres.merge(dbToMerge);
                            logOutput(`POSTGRES.MERGE completed with ${dbToMerge}`);
                        }
                    }
                    break;

                case command.startsWith('PIG.MERGE'):
                    {
                        const match = command.match(/PIG\.MERGE\(([^)]+)\)/);
                        if (match) {
                            const dbToMerge = match[1].trim();
                            await pig.merge(dbToMerge);
                            logOutput(`PIG.MERGE completed with ${dbToMerge}`);
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

main().catch(err => console.error('Fatal error:', err));
