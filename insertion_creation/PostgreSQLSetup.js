const { Client } = require('pg');
const fs = require('fs');
const csv = require('csv-parser');
const dotenv = require('dotenv');

// PostgreSQL connection
const connectPostgres = async (config) => {
    const client = new Client(config);
    try {
        await client.connect();
        console.log(`PostgreSQL connected to ${config.database}`);
        return client;
    } catch (error) {
        console.error(`Error connecting to PostgreSQL: ${error.message}`);
        process.exit(1);
    }
};

// Load CSV to PostgreSQL
async function loadData({ config, tableName, csvFilePath, columnMapping, schema}) {
    const client = await connectPostgres(config);

    // Drop table if exists
    await client.query(`DROP TABLE IF EXISTS ${tableName}`);
    console.log(`Table "${tableName}" dropped`);

    // Create table
    await client.query(schema);
    console.log(`Table "${tableName}" created`);

    const csvData = [];

    fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
            const formattedRow = {};
            for (const [key, value] of Object.entries(columnMapping)) {
                formattedRow[key] = row[value];
            }
            csvData.push(formattedRow);
        })
        .on('end', async () => {
            console.log('CSV file successfully processed');
            try {
                for (const row of csvData) {
                    await client.query(
                        `INSERT INTO ${tableName} (studentid, courseid, rollno, email, grade) VALUES ($1, $2, $3, $4, $5)`,
                        [row.studentID, row.courseID, row.rollno, row.email, row.grade]
                    );
                }
                console.log(`Data inserted successfully into table: ${tableName}`);
            } catch (error) {
                console.error('Error inserting data:', error);
            } finally {
                await client.end();
                console.log('PostgreSQL connection closed');
            }
        });
}

// Run only when this file is executed directly
if (require.main === module) {

    // Reading config from .env file
    dotenv.config();
    const user = process.env.POST_USER;
    const host = process.env.POST_HOST;
    const databaseName = process.env.POST_DBNAME;
    const password = process.env.POST_PASSWORD;
    const port = process.env.POST_PORT;
    const tableName = 'studentgrades';
    const csvFilePath = process.env.POST_CSV || 'student_course_grades.csv';
    

    const schema = 
        `CREATE TABLE ${tableName} (
            studentid TEXT,
            courseid TEXT,
            rollno TEXT,
            email TEXT,
            grade TEXT NOT NULL,
            PRIMARY KEY (studentid, courseid)
        );`;

    const config = {
        user: user,
        host: host,
        database: databaseName,
        password: password,
        port: port,
    };

    const columnMapping = {
        studentID: 'student-ID',
        courseID: 'course-id',
        rollno: 'roll no',
        email: 'email ID',
        grade: 'grade',
    };

    loadData({
        config,
        tableName,
        csvFilePath,
        columnMapping,
        schema
    }).catch((err) => {
        console.error(`Error loading data: ${err.message}`);
    });
}

module.exports = { connectPostgres, loadData };
