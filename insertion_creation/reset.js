fs = require('fs').promises;
fs.writeFile('OpLogs/Mongo_LastSync.json', JSON.stringify({ LstSyncWithPig: 0, LstSyncWithPostgres: 0 }))
fs.writeFile('OpLogs/Postgres_LastSync.json', JSON.stringify({ LstSyncWithPig: 0, LstSyncWithMongo: 0 }))
fs.writeFile('OpLogs/Pig_LastSync.json', JSON.stringify({ LstSyncWithMongo: 0, LstSyncWithPostgres: 0 }))

fs.writeFile('OpLogs/opLog_MongoDB.jsonl', '')
fs.writeFile('OpLogs/opLog_Postgres.jsonl', '')
fs.writeFile('OpLogs/opLog_Pig.jsonl', '')

