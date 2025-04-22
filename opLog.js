const fs = require('fs').promises;



async function insertToOpLog(collectionName,fieldName,operationName, operationData, dbType) {
    const operation = {
        collection: collectionName,
        field: fieldName,
        timestamp: new Date().getTime(),
        type: operationName,
        data: operationData,
    };

    const logLine = JSON.stringify(operation) + '\n';
    console.log('Logging operation:', logLine);
    try {
        await fs.appendFile(`OpLogs/opLog_${dbType}.jsonl`, logLine);
        console.log('Operation logged:', operation);
    } catch (err) {
        console.error('Error writing to opLog:', err);
    }
}

async function flushOpLog(dbType, newOps) {
    const opLogFile = `OpLogs/opLog_${dbType}.jsonl`;
    const result = newOps.map(op => JSON.stringify(op)).join('\n') + '\n';

    try {
        await fs.writeFile(opLogFile, result);
        console.log('OpLog flushed:', opLogFile);
    } catch (err) {
        console.error('Error writing to opLog:', err);
    }
}

        
        
async function readFromOpLog(dbType) {
    const opLogFile = `OpLogs/opLog_${dbType}.jsonl`;
    const readline = require('readline');
    const fsStream = require('fs');

    const operations = [];

    return new Promise((resolve, reject) => {
        const stream = fsStream.createReadStream(opLogFile);
        const rl = readline.createInterface({
            input: stream,
            crlfDelay: Infinity
        });

        rl.on('line', (line) => {
            try {
                const operation = JSON.parse(line);
                operations.push(operation);
            } catch (err) {
                console.error('Error parsing line:', err);
            }
        });

        rl.on('close', () => {
            resolve(operations);
        });

        rl.on('error', (err) => {
            reject(err);
        });
    });
}


module.exports = {
    insertToOpLog,
    flushOpLog,
    readFromOpLog
};