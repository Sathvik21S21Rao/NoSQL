// main.js (single file)
const { Client } = require('pg');
const { insertToOpLog, readFromOpLog, flushOpLog } = require('./opLog');
const { model } = require('mongoose');

class PostgreSQLOps {

    constructor(config) {
        this.client = new Client(config);
        this.client.connect()
            .then(() => console.log('Connected to PostgreSQL!'))
            .catch(err => console.error('Connection error', err.stack));
        this.dbname = config.database;
        this.LstSyncWithPig = new Date(0).getTime();
        this.LstSyncWithMongo = new Date(0).getTime();
    }

    async performOperation(table, fieldName, operation, data) {
        const tableName = table;
        let result;
        console.log('Performing operation:', operation, 'with data:', data);

        switch (operation) {
            case 'insert':
                result = await this.runQuery(
                    `INSERT INTO ${tableName} (studentid, courseid, grade) VALUES ($1, $2, $3) RETURNING *`,
                    [data.studentID, data.courseID, data.grade]
                );
                await insertToOpLog(this.dbname, fieldName, 'insert', data, 'Postgres');
                break;

            case 'update':
                result = await this.runQuery(
                    `UPDATE ${tableName} SET ${fieldName} = $1 WHERE studentid = $2 AND courseid = $3 RETURNING *`,
                    [data.grade, data.studentID, data.courseID]
                );
                await insertToOpLog(this.dbname, fieldName, 'update', data, 'Postgres');
                break;


            default:
                throw new Error('Invalid operation');
        }

        return result;
    }

    async readRecord(tableName, studentID, courseID) {
        const query = 'SELECT * FROM ' + tableName + ' WHERE studentID = $1 AND courseID = $2';
        try {
            const res = await this.client.query(query, [studentID, courseID]);
            if (res.rows.length > 0) {
                console.log('Record found:', res.rows[0]);
            } else {
                console.log('Record not found.');
            }
        } catch (err) {
            console.error('Error reading record:', err);
        }
    }


    mergeSortedLogs(log1, log2) {
        let merged = [];
        let i = 0, j = 0;

        while (i < log1.length && j < log2.length) {
            const t1 = new Date(log1[i].timestamp).getTime();
            const t2 = new Date(log2[j].timestamp).getTime();

            if (t1 <= t2) {
                merged.push(log1[i]);
                i++;
            } else {
                merged.push(log2[j]);
                j++;
            }
        }

        // Add any remaining entries
        while (i < log1.length) merged.push(log1[i++]);
        while (j < log2.length) merged.push(log2[j++]);

        return merged;
    }

    async merge(dbType) {
        const operations = await readFromOpLog(dbType);
        const postgresOpLog = await readFromOpLog('Postgres');

        if (!operations || operations.length === 0) {
            console.log('No operations to merge');
            return;
        }

        let lastSyncTime = 0;

        if (dbType === 'Pig') {
            lastSyncTime = this.LstSyncWithPig;
        }

        else if (dbType === 'MongoDB') {
            lastSyncTime = this.LstSyncWithMongo;
        }

        let low = 0;
        let high = operations.length - 1;

        while (low <= high) {
            let mid = Math.floor((low + high) / 2);
            let timestamp = operations[mid].timestamp;

            if (timestamp === lastSyncTime) {
                low = mid + 1;
                break;
            } else if (timestamp < lastSyncTime) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        let newOps = [];



        for (let i = low; i < operations.length; i++) {
            const op = operations[i];
            const { timestamp, collection, field, type, data } = operation[i];
            try {
                newOps.push(op);
            } catch (error) {
                console.error('Error merging operation:', error);
            }
        }

        let sortedOps = this.mergeSortedLogs(newOps, postgresOpLog);

        low = 0;
        high = sortedOps.length - 1;

        while (low <= high) {
            let mid = Math.floor((low + high) / 2);
            let timestamp = sortedOps[mid].timestamp;

            if (timestamp === lastSyncTime) {
                low = mid + 1;
                break;
            } else if (timestamp < lastSyncTime) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        for (let i = low; i < sortedOps.length; i++) {
            const operation = sortedOps[i];
            try {
                await this.performOperation(operation.collection, operation.field, operation.type, operation.data);
            } catch (err) {
                console.error('Error performing operation:', err);
            }
        }

        if (dbType === 'Pig') {
            this.LstSyncWithPig = sortedOps[sortedOps.length - 1].timestamp;
        }
        else if (dbType === 'MongoDB') {
            this.LstSyncWithMongo = sortedOps[sortedOps.length - 1].timestamp;
        }
        await flushOpLog("Postgres", sortedOps);

        console.log('Merged operations:', sortedOps);

        try {
            await fs.writeFile('OpLogs/Postgres_LastSync.json', JSON.stringify({ LstSyncWithPig: this.LstSyncWithPig, LstSyncWithMongo: this.LstSyncWithMongo }));
        }

        catch (err) {
            console.error('Error writing to file:', err);
        }
    }


    async runQuery(query, params = []) {
        try {
            const res = await this.client.query(query, params);
            return res.rows;
        } catch (err) {
            console.error('Query error:', err);
            throw err;
        }
    }

    async close() {
        await this.client.end();
        console.log('PostgreSQL connection closed.');
    }
}

async function testQueries() {
    const db = new PostgreSQLOps({
        host: '127.0.0.1',
        port: 5432,
        user: 'vishruthvijay',
        password: '',
        database: 'studentgrades',
    });


    try {
        await db.performOperation('studentgrades', 'grade', 'update', { studentID: 'SID1033', courseID: 'CSE016', grade: 'D' });
    } catch (error) {
        console.error('Error performing operation:', error);
    } finally {
        await db.close();
    }
}

// testQueries();
module.exports = PostgreSQLOps;
