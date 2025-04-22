const { exec } = require('child_process');
const fs = require('fs');

const CSV_filepath = './student_course_grades.csv';
const pigScriptPath = './student_grades.pig';

const HDFS_put = `hdfs dfs -put ${CSV_filepath} /user/hadoop1/`;
const pigCommand = `pig -x mapreduce -f ${pigScriptPath}`;

function executePigCommand() {

    exec(HDFS_put, (err, stdout, stderr) => {
        if (err) {
        console.error(`Error executing HDFS Put command: ${err.message}`);
        return;
        }
        if (stderr) {
        console.error(`HDFS Put command stderr: ${stderr}`);
        return;
        }
        console.log(`HDFS Put command executed successfully: ${stdout}`);
    });

    exec(pigCommand, (err, stdout, stderr) => {
        if (err) {
        console.error(`Error executing Pig command: ${err.message}`);
        return;
        }
        if (stderr) {
        console.error(`Pig command stderr: ${stderr}`);
        return;
        }
        console.log(`Pig command executed successfully: ${stdout}`);
    });
}

async function run() {
  try {
    executePigCommand();
  } catch (err) {
    console.error('Error:', err.message);
  }
}

run();
