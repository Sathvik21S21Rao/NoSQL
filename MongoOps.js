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
                default:
                    throw new Error('Invalid operation');
            }
            return result;
        }

        async merge(dbType) {
            const fs = require('fs');
            
            const operations =await readFromOpLog(dbType);
            const mongoOpLog=await readFromOpLog('MongoDB');

            if(operations.length==0) {
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

            let low=0;
            let high=operations.length-1;
            lastSyncTime=new Date(lastSyncTime).getTime();
        

            while (low <= high) {
                let mid = Math.floor((low + high) / 2);
                let timestamp = new Date(operations[mid].timestamp).getTime();
                if (timestamp === lastSyncTime) {
                    low = mid;
                    break;
                } else if (timestamp < lastSyncTime) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            
            for (let i = low; i < operations.length; i++) {
                const operation = operations[i];
                const { timestamp,type, data } = operation;
                timestamp = new Date(timestamp).getTime();
                if(timestamp>lastSyncTime) {
                    try {
                        if ((type=='update' && mongoOpLog.some(op=> op.data.studentID==data.studentID && op.data.courseID==data.courseID)) || type!='update') 
                            await this.performOperation(type, data);
                        mongoOpLog.push(operation);
                    } catch (err) {
                        console.error('Error performing operation:', err);
                    }
                }
                                
            }

            sortedOps = mongoOpLog.sort((a, b) => new Date(a.timestamp) < new Date(b.timestamp));

            if(dbType == 'Pig') {
                this.LstSyncWithPig = operations[operations.length - 1].timestamp;
            }
            else if(dbType == 'Postgres') {
                this.LstSyncWithPostgres = operations[operations.length - 1].timestamp;
            }
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