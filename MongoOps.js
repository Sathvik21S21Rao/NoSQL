const { insertToOpLog, readFromOpLog, flushOpLog } = require('./opLog');

class MongoOps {
    constructor(url){
        this.url = url;
        this.mongoose = require('mongoose');
        this.mongoose.set('strictQuery', false);
        this.mongoose.connect(url, { useNewUrlParser: true, useUnifiedTopology: true })
            .then(() => {
                console.log('MongoDB connected');
            })
            .catch((err) => {
                console.error('MongoDB connection error:', err);
            });
        this.LstSyncWithPig=0;
        this.LstSyncWithPostgres=0;
        }

        async performOperation(operation, data) {
            const collectionName = 'studentgrades';
            const collection = this.mongoose.connection.collection(collectionName);
            let result;
            console.log('Performing operation:', operation, 'with data:', data);
            switch (operation) {
                case 'insert':
                    result = await collection.insertOne(data);
                    await insertToOpLog('insert', data, 'MongoDB');
                    break;
                case 'update': // to update only grade field
                    result = await collection.updateOne({ studentID: data.studentID, courseID: data.courseID }, { $set: { grade: data.grade } });
                    await insertToOpLog('update', data, 'MongoDB');
                    break;
                case 'delete':
                    result = await collection.deleteMany({ studentID: data.studentID });
                    await insertToOpLog('delete', data, 'MongoDB');
                    break;
                default:
                    throw new Error('Invalid operation');
            }
            return result;
        }

        async merge(dbType) {
            const fs = require('fs');
            
            const operations =await readFromOpLog(dbType);
            const mongoOpLog=await readFromOpLog('MongoDB');
       
            for (const operation of operations) {
                const { timestamp,type, data } = operation;
                if(timestamp>this.LstSyncWithPig && dbType=='Pig' || timestamp>this.LstSyncWithPostgres && dbType=='Postgres'){
                    try {
                        if ((type=='update' && mongoOpLog.some(op=> op.data.studentID==data.studentID && op.data.courseID==data.courseID && op.timestamp<timestamp)) || type!='update') 
                            await this.performOperation(type, data);
                        mongoOpLog.push(operation);
                    } catch (err) {
                        console.error('Error performing operation:', err);
                    }
                }
                
                //write the sorted operations to a new file
                
            }
            sortedOps = mongoOpLog.sort((a, b) => new Date(a.timestamp) < new Date(b.timestamp));
          
            await flushOpLog("MongoDB",sortedOps);

        }
        async close() {
            await this.mongoose.connection.close();
            console.log('MongoDB connection closed');
        }
}
// do an example update operation
const mongoOps = new MongoOps('mongodb://localhost:27017/testdb');
const data = {
    studentID: 'SID1033',
    courseID: 'CSE016',
    grade: 'B',
};
mongoOps.performOperation('update', data)
    .then(() => {
        console.log('Operation completed successfully');
    })
    .catch((err) => {
        console.error('Error performing operation:', err);
    })
mongoOps.close()
    .then(() => {
        console.log('MongoDB connection closed');
    })
    .catch((err) => {
        console.error('Error closing MongoDB connection:', err);
    });