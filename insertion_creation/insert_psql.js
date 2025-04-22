const { Client } = require('pg');
const fs = require('fs');
const csv = require('csv-parser');

const dbName = 'studentgrades';
const tableName = 'studentgrades';

const adminClient = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'pwd',
  port: 5432,
});

async function setupAndLoad() {
  try {
    await adminClient.connect();
    console.log('Connected');

    await adminClient.query(`
      SELECT pg_terminate_backend(pid) 
      FROM pg_stat_activity 
      WHERE datname = '${dbName}' AND pid <> pg_backend_pid();
    `);

    await adminClient.query(`DROP DATABASE IF EXISTS ${dbName}`);
    await adminClient.query(`CREATE DATABASE ${dbName}`);
    console.log(`Database ${dbName} created`);

    await adminClient.end();

    function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

    await delay(1000);

    const client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: dbName,
      password: 'pwd',
      port: 5432,
    });

    await client.connect();
    console.log(`Connected to ${dbName}`);
    await client.query(`DROP TABLE IF EXISTS ${tableName}`);

    await client.query(
        `CREATE TABLE ${tableName} 
        (
            studentid VARCHAR,
            courseid  VARCHAR,
            rollno    VARCHAR,
            email      VARCHAR,
            grade      VARCHAR
        );`
    );
    console.log(`Table ${tableName} created`);

    const csvFilePath = './student_course_grades.csv';
    const insertPromises = [];
    
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        const query = {
          text: `INSERT INTO ${tableName} (studentid, courseid, rollno, email, grade) VALUES ($1, $2, $3, $4, $5)`,
          values: [
            row['student-ID'],
            row['course-id'],
            row['roll no'],
            row['email ID'],
            row['grade'],
          ],
        };
        insertPromises.push(client.query(query));
      })
      .on('end', async () => {
        try {
          await Promise.all(insertPromises);
          console.log('CSV data inserted successfully');
        } catch (err) {
          console.error('Error inserting data:', err.message);
        } finally {
          await client.end();
        }
      });

  } catch (err) {
    console.error('Error:', err.message);
    await adminClient.end();
  }
}

setupAndLoad();
