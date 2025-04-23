const { mongo, model } = require('mongoose');
const { insertToOpLog, readFromOpLog, flushOpLog } = require('./opLog');
const fs = require('fs').promises;

class MongoOps {
    constructor(url) {
        this.url = url;
        this.mongoose = require('mongoose');
        this.mongoose.set('strictQuery', false);
        this.LstSyncWithPig = new Date(0).getTime();
        this.LstSyncWithPostgres = new Date(0).getTime();
    }

    async connect() {
        try {
            await this.mongoose.connect(this.url, { useNewUrlParser: true, useUnifiedTopology: true });
            console.log('MongoDB connected');
        } catch (error) {
            console.error('Error connecting to MongoDB:', error);
        }

        try {
            const data = await fs.readFile("OpLogs/Mongo_LastSync.json", 'utf8');
            const json = JSON.parse(data);
            console.log('Read from file:', json);
            this.LstSyncWithPig = json.LstSyncWithPig;
            this.LstSyncWithPostgres = json.LstSyncWithPostgres;
            console.log('Last sync times:', this.LstSyncWithPig, this.LstSyncWithPostgres);
        } catch (err) {

        }

    }

    async performOperation(collectionName, fieldName, operation, data) {

        const collection = this.mongoose.connection.collection(collectionName);
        let result;
        console.log('Performing operation:', operation, 'with data:', data);
        switch (operation) {
            case 'insert':
                result = await collection.insertOne(data);
                await insertToOpLog(collectionName, fieldName, 'insert', data, 'MongoDB');
                break;
            case 'update': // to update only grade field
                console.log('collection', collectionName, 'Updating field:', fieldName, 'with value:', data[fieldName]);

                result = await collection.updateOne(
                    { studentID: data.studentID, courseID: data.courseID },
                    { $set: { [fieldName]: data[fieldName] } }
                );

                await insertToOpLog(collectionName, fieldName, 'update', data, 'MongoDB');
                break;
            default:
                throw new Error('Invalid operation');
        }
        return result;
    }

    async readRecord(collectionName, studentID, courseID) {
        const collection = this.mongoose.connection.collection(collectionName);
        try {
            const result = await collection.findOne({ studentID, courseID });
            if (result) {
                console.log('Record found:', result);
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
        const mongoOpLog = await readFromOpLog('MongoDB');

        if (operations.length == 0) {
            console.log('No operations to merge');
            return;
        }

        let lastSyncTime = 0;

        if (dbType == 'Pig') {
            lastSyncTime = this.LstSyncWithPig;
        }

        else if (dbType == 'Postgres') {
            lastSyncTime = this.LstSyncWithPostgres;
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

        let newOps = []

        for (let i = low; i < operations.length; i++) {
            const operation = operations[i];
            try {
                newOps.push(operation);
            } catch (err) {
                console.error('Error performing operation:', err);
            }

        }

        let sortedOps = this.mergeSortedLogs(newOps, mongoOpLog);

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

        if (dbType == 'Pig') {
            this.LstSyncWithPig = operations[operations.length - 1].timestamp;
        }
        else if (dbType == 'Postgres') {
            this.LstSyncWithPostgres = operations[operations.length - 1].timestamp;
        }

        await flushOpLog("MongoDB", sortedOps);

        try {
            await fs.writeFile("OpLogs/Mongo_LastSync.json", JSON.stringify({ LstSyncWithPig: this.LstSyncWithPig, LstSyncWithPostgres: this.LstSyncWithPostgres }));
        }
        catch (err) {
            console.error('Error writing to file:', err);
        }

    }
    async close() {
        await this.mongoose.connection.close();
        console.log('MongoDB connection closed');
    }
}


async function mergerun() {
    const mongoOps = new MongoOps('mongodb://localhost:27017/StudentGrades');

    try {
        await mongoOps.connect();
        await mongoOps.merge('Postgres');
        console.log('Merge completed successfully');
    } catch (err) {
        console.error('Error during merge:', err);
    } finally {
        await mongoOps.close();
    }
}

async function updateMongo() {
    const mongoOps = new MongoOps('mongodb://localhost:27017/StudentGrades');

    try {
        await mongoOps.connect();
        await mongoOps.performOperation('studentgrades', 'grade', 'update', { studentID: 'SID1033', courseID: 'CSE016', grade: 'A+' });
        console.log('Update completed successfully');
    } catch (err) {
        console.error('Error during update:', err);
    } finally {
        await mongoOps.close();
    }
}
async function run() {
    await mergerun();
    await mergerun();


}
// run();
module.exports = MongoOps;