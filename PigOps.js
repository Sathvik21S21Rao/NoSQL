const { exec } = require('child_process');
const fs = require('fs').promises;
const path = require('path');
const { insertToOpLog, readFromOpLog, flushOpLog } = require('./opLog');

const Pig_HDFS_Path = '/user/hadoop1'
const HADOOP_HOME_var = '/home/hadoop1/hadoop'
const JAVA_HOME_var = '/usr/lib/jvm/java-21-openjdk-amd64'
const PATH_env_var = ':/home/siddharth/Downloads/nosql/assn3/pig/pig/bin'
const CollectionName = 'studentgrades'
const pigScriptPath = './update_script.pig'


async function execCommand(command) {
    return new Promise((resolve, reject) => {
      exec(command, (err, stdout, stderr) => {
        if (err) {
          console.error(`Error executing: ${command}\n${err.message}`);
          return reject(err);
        }
        if (stderr) console.error(`stderr: ${stderr}`);
        console.log(`stdout for "${command}":\n${stdout}`);
        resolve(stdout);
      });
    });
  }
  

class PigOps {
    constructor(outputHdfsPath) {
        this.outputHdfsPath = outputHdfsPath;
        this.LstSyncWithMongo = new Date(0).getTime();
        this.LstSyncWithPostgres = new Date(0).getTime();
        this.tempOutputPath = `${Pig_HDFS_Path}/temp`
        this.PigFolder = Pig_HDFS_Path
    }


    async initializePigOps() {
        try 
        {
            const data = await fs.readFile("OpLogs/Pig_LastSync.json", 'utf8');
            const json = JSON.parse(data);
            this.LstSyncWithMongo = json.LstSyncWithMongo || new Date(0).getTime();
            this.LstSyncWithPostgres = json.LstSyncWithPostgres || new Date(0).getTime();
            console.log('Last sync times:', this.LstSyncWithPig, this.LstSyncWithPostgres);
            // execCommand("hdfs dfs -rm -r /user/hadoop1/temp");
        } 
        catch (err) 
        {
            console.warn('No previous sync file found or error reading sync file. Or there was an error removing temp');
        }
    }


    async writePigScript(inputHdfsPath, pigScriptPath, fieldName, operation, data) 
    {
        if (operation === 'update') 
        {
            const studentID = data.studentID;
            const courseID = data.courseID;
            const newGrade = data[fieldName];
    
            const pigScript = `
                student_data = LOAD '${inputHdfsPath}' USING PigStorage(',') 
                    AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);
    
                student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == '${studentID}' AND course_id == '${courseID}' ? '${newGrade}' : grade) AS grade;
    
                STORE student_data_new INTO '${this.tempOutputPath}' USING PigStorage(',');
            `;

            await fs.writeFile(pigScriptPath, pigScript, 'utf8');
            console.log('Pig script written to file:', pigScriptPath);
        }


        if (operation === "read")
        {
            const studentID = data.studentID;
            const courseID = data.courseID;

            const pigScript = `
            student_data = LOAD '${inputHdfsPath}' USING PigStorage(',') 
                AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);

            student_data_selected = FILTER student_data BY
                student_id == '${studentID}' AND course_id == '${courseID}';

            DUMP student_data_selected;
        `;

        await fs.writeFile(pigScriptPath, pigScript, 'utf8');
            console.log('Pig script written to file:', pigScriptPath);
        }
    }

    async executePigCommand(pigScriptPath) {
        // console.log("here");

        const pigCommand = `pig -x mapreduce -f ${pigScriptPath}`;
        const env = {
            ...process.env,
            PATH: process.env.PATH + PATH_env_var,
            JAVA_HOME: process.env.JAVA_HOME || JAVA_HOME_var,
            HADOOP_HOME: process.env.HADOOP_HOME || HADOOP_HOME_var
        };

        await execCommand(pigCommand);
    }


    async updatePig(inputHdfsPath, fieldName, operation, data, timestamp)
    {
        await this.writePigScript(inputHdfsPath, pigScriptPath, fieldName, operation, data);

        try {

        // console.log("executing written command now");
        await this.executePigCommand(pigScriptPath);
        // console.log("here");


        const backupPath = `${this.PigFolder}/${inputHdfsPath}.bak`;
        
        await execCommand(`hdfs dfs -cp ${this.PigFolder}/${inputHdfsPath} ${backupPath}`);

        await execCommand(`hdfs dfs -rm -r ${this.PigFolder}/${inputHdfsPath}`);

        await execCommand(`hdfs dfs -mv ${this.tempOutputPath}/part-m-00000 ${this.PigFolder}/${inputHdfsPath}`);

        await execCommand(`hdfs dfs -rm -r ${this.tempOutputPath}`);

        await execCommand(`hdfs dfs -rm -r ${backupPath}`);

        await insertToOpLog(inputHdfsPath, fieldName, operation, data, 'Pig', timestamp);
        console.log("Operation logged successfully.");
    
        } 
        catch (err) 
        {
            console.error("Error during Pig update, initiating rollback:", err.message);
            try 
            {
                const backupPath = `${this.PigFolder}/${inputHdfsPath}.bak`;
                await execCommand(`hdfs dfs -cp ${backupPath} ${this.PigFolder}/${inputHdfsPath}`);
                await execCommand(`hdfs dfs -rm ${backupPath}`);
                try
                {
                    await execCommand(`hdfs dfs -rm -r ${this.tempOutputPath}`);
                }
                catch (err) {};
                console.log("Rollback successful");
            }
            
            catch (rollbackErr)
            {
                console.error("Rollback failed:", rollbackErr.message);
            }
    
            throw err;
        }
    }

    async readRecord(inputHdfsPath, studentID, courseID) 
    {
        const pigScriptPath = './update_script.pig';
        await this.writePigScript(inputHdfsPath,pigScriptPath,null,"read",{ studentID: studentID, courseID: courseID });
        await this.executePigCommand(pigScriptPath);
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

    async merge(dbType) 
    {

        const operations = await readFromOpLog(dbType);
        const PigOpLog = await readFromOpLog('Pig');

        if (operations.length == 0) 
        {
            console.log('No operations to merge');
            return;
        }

        let lastSyncTime = 0;

        if (dbType == 'Mongo') 
        {
            lastSyncTime = this.LstSyncWithMongo;
        }

        else if (dbType == 'Postgres') 
        {
            lastSyncTime = this.LstSyncWithPostgres;
        }
        
        let low = 0;
        let high = operations.length - 1;

        while (low <= high) 
        {
            let mid = Math.floor((low + high) / 2);
            let timestamp = operations[mid].timestamp;

            if (timestamp === lastSyncTime) 
            {
                low = mid + 1;
                break;
            } 
            
            else if (timestamp < lastSyncTime) 
            {
                low = mid + 1;
            } 
            
            else 
            {
                high = mid - 1;
            }
        }

        let newOps = []

        for (let i = low; i < operations.length; i++) 
        {
            const operation = operations[i];
            try 
            {
                newOps.push(operation);
            }
            catch (err) 
            {
                console.error('Error performing operation:', err);
            }

        }

        let sortedOps = this.mergeSortedLogs(newOps, PigOpLog);

        low = 0;
        high = sortedOps.length - 1;

        while (low <= high) 
        {
            let mid = Math.floor((low + high) / 2);
            let timestamp = sortedOps[mid].timestamp;

            if (timestamp === lastSyncTime) 
            {
                low = mid + 1;
                break;
            } 
            
            else if (timestamp < lastSyncTime) 
            {
                low = mid + 1;
            }

            else 
            {
                high = mid - 1;
            }
        }

        await this.BulkPigFileWriteStart(CollectionName);
        for (let i = low; i < sortedOps.length; i++) 
        {
            const operation = sortedOps[i];
            try 
            {
                await this.BulkPigFileWriteAppend(operation.collection, operation.field, operation.type, operation.data);
            } 
            catch (err)
            {
                console.error('Error performing operation:', err);
            }
        }
        await this.BulkPigFileWriteEnd(CollectionName);

        try {

            await this.executePigCommand(pigScriptPath);    
    
            const backupPath = `${this.PigFolder}/${CollectionName}.bak`;
            
            await execCommand(`hdfs dfs -cp ${this.PigFolder}/${CollectionName} ${backupPath}`);
    
            await execCommand(`hdfs dfs -rm -r ${this.PigFolder}/${CollectionName}`);
    
            await execCommand(`hdfs dfs -mv ${this.tempOutputPath}/part-m-00000 ${this.PigFolder}/${CollectionName}`);
    
            await execCommand(`hdfs dfs -rm -r ${this.tempOutputPath}`);
    
            await execCommand(`hdfs dfs -rm -r ${backupPath}`);
    
            // await insertToOpLog(inputHdfsPath, fieldName, operation, data, 'Pig', timestamp);
            console.log("Operation logged successfully.");
        
            } 

            catch (err) 
            {
                console.error("Error during Pig update, initiating rollback:", err.message);
                try 
                {
                    const backupPath = `${this.PigFolder}/${CollectionName}.bak`;
                    await execCommand(`hdfs dfs -cp ${backupPath} ${this.PigFolder}/${CollectionName}`);
                    await execCommand(`hdfs dfs -rm ${backupPath}`);
                    try
                    {
                        await execCommand(`hdfs dfs -rm -r ${this.tempOutputPath}`);
                    }
                    catch (err) {};
                    console.log("Rollback successful");
                }
                
                catch (rollbackErr)
                {
                    console.error("Rollback failed:", rollbackErr.message);
                }
        
                throw err;
            }


        if (dbType == 'MongoDB') 
        {
            this.LstSyncWithMongo = operations[operations.length - 1].timestamp;
        }
        else if (dbType == 'Postgres') 
        {
            this.LstSyncWithPostgres = operations[operations.length - 1].timestamp;
        }

        await flushOpLog("Pig", sortedOps);

        try 
        {
            await fs.writeFile("OpLogs/Pig_LastSync.json", JSON.stringify({ LstSyncWithMongo: this.LstSyncWithMongo, LstSyncWithPostgres: this.LstSyncWithPostgres }));
        }
        catch (err) 
        {
            console.error('Error writing to file:', err);
        }

    }


    async performOperation(inputHdfsPath, field, type, data, timestamp=null)
    {
        if (type === "update")
        {
            await this.updatePig(inputHdfsPath, field, type, data, timestamp);
        }
        if (type === "read")
        {
            await this.readRecord(inputHdfsPath, field, type, data);
        }
    }

    async BulkPigFileWriteStart(inputHdfsPath)
    {  
        const pigScript = `
            student_data = LOAD '${inputHdfsPath}' USING PigStorage(',') 
                AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);
        `;

        await fs.writeFile(pigScriptPath, pigScript, 'utf8');
        console.log('Pig script written to file:', pigScriptPath)
    }

    async BulkPigFileWriteAppend(inputHdfsPath, fieldName, operation, data)
    {  
        const studentID = data.studentID;
        const courseID = data.courseID;
        const newGrade = data[fieldName];

        const pigScript = `
             student_data = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == '${studentID}' AND course_id == '${courseID}' ? '${newGrade}' : grade) AS grade;
        `;

        await fs.appendFile(pigScriptPath, pigScript, 'utf8');
        console.log('Pig script written to file:', pigScriptPath)
    }

    async BulkPigFileWriteEnd(inputHdfsPath)
    {
        const pigScript = `
            STORE student_data INTO '${this.tempOutputPath}' USING PigStorage(',');
        `;
        await fs.appendFile(pigScriptPath, pigScript, 'utf8');
        console.log('Pig script written to file:', pigScriptPath);
    }

}

// async function runPigOps() 
// {
//     const pigOps = new PigOps(`${Pig_HDFS_Path}/${CollectionName}`);
//     await pigOps.initializePigOps();
//     try 
//     {
//         await pigOps.updatePig('studentgrades', 'grade', 'update', { studentID: 'SID1033', courseID: 'CSE016', grade: 'A+' }, 1)
//         // await pigOps.readRecord('student_course_grades.csv', 'SID1033', 'CSE016');
//         await pigOps.merge("Postgres");
//     } 
//     catch (err) 
//     {
//         console.error('PigOps error:', err);
//     }
// }

// runPigOps();


// comments for reference:
//      Pig does not take in any schema/collection name, it just takes in .csv file name. I have tested it with 'student_course_grades.csv'
//      in HDFS, after insertion, it is stored in 

module.exports = PigOps;
