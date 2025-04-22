const { Client } = require('pg');
const fs = require('fs');
const csv = require('csv-parser');

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
async function loadData({ config, tableName, csvFilePath, columnMapping }) {
    const client = await connectPostgres(config);

    // Drop table if exists
    await client.query(`DROP TABLE IF EXISTS ${tableName}`);
    console.log(`Table "${tableName}" dropped`);

    // Create table
    await client.query(`
        CREATE TABLE ${tableName} (
            studentid TEXT,
            courseid TEXT,
            rollno TEXT,
            email TEXT,
            grade TEXT NOT NULL,
            PRIMARY KEY (studentid, courseid)
        );
    `);
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
    const config = {
        user: 'myuser',
        host: '127.0.0.1',
        database: 'studentgrades',
        password: 'nosql',
        port: 5432,
    };

    const tableName = 'studentgrades';
    const csvFilePath = './student_course_grades.csv';

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
        columnMapping
    }).catch((err) => {
        console.error(`Error loading data: ${err.message}`);
    });
}

module.exports = { connectPostgres, loadData };
