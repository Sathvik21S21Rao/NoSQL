const { exec } = require('child_process');
const fs = require('fs');

const CSV_filepath = './student_course_grades.csv';
const pigScriptPath = './student_grades.pig';
const CSV_hdfs_filepath = 'studentgrades';
const Pig_HDFS_Path = '/user/hadoop1/'

const HDFS_put = `hdfs dfs -put ${CSV_filepath} ${Pig_HDFS_Path}`;
const HDFS_rename = `hdfs dfs -mv ${Pig_HDFS_Path}${CSV_filepath} ${Pig_HDFS_Path}studentgrades`;
const pigCommand = `pig -x mapreduce -f ${pigScriptPath}`;

function execCommand(command) 
{
  return new Promise((resolve, reject) => 
    {
    exec(command, (err, stdout, stderr) => 
      {
      if (err) 
        {
        console.error(`Error executing: ${command}\n${err.message}`);
        return reject(err);
        }
      if (stderr) console.error(`stderr: ${stderr}`);
      console.log(`stdout for "${command}":\n${stdout}`);
      resolve(stdout);
    });
  });
}


// What is happening here:
//      puts csv file into hdfs at /user/hadoop1/student_course_grades.csv
//      executes pig, which saves data into /student-data/part-m-00000
//      removes /user/hadoop1/student_course_grades.csv
//      moves part-m-00000 (pig output) to /user/hadoop1/student_course_grades.csv
//      deletes /student_data temp folder

async function executePigCommand() 
{
  await execCommand(HDFS_put);
  await execCommand(HDFS_rename);
  await execCommand(pigCommand);
  await execCommand(`hdfs dfs -rm -r /user/hadoop1/${CSV_hdfs_filepath}`);
  await execCommand(`hdfs dfs -mv /student_data/part-m-00000 /user/hadoop1/${CSV_hdfs_filepath}`);
  await execCommand(`hdfs dfs -rm -r /student_data`);
}


async function run() {
  try {
    executePigCommand();
  } catch (err) {
    console.error('Error:', err.message);
  }
}

run();
