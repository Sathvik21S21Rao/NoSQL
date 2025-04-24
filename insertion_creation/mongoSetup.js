const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');

mongoose.set('strictQuery', false); // Suppress deprecation warning

const connectMongo = async (url) => {
    try {
        await mongoose.connect(url, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        console.log(`MongoDB connected to ${url}`);
    } catch (error) {
        console.error(`Error connecting to MongoDB: ${error.message}`);
        process.exit(1);
    }
};


async function loadData({ url, collectionName, schema, csvFilePath, columnMapping }) {
    await connectMongo(url);

    const db = mongoose.connection;
    const collections = await db.db.listCollections({ name: collectionName }).toArray();
    if (collections.length > 0) {
        await db.dropCollection(collectionName);
        console.log(`Collection "${collectionName}" dropped`);
    } else {
        console.log(`Collection "${collectionName}" does not exist`);
    }

    const Model = mongoose.model(collectionName, schema);

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
            await Model.insertMany(csvData);
            console.log('Data inserted successfully into collection:', collectionName);
            mongoose.connection.close();
        });
}

if (require.main === module) {
    const dbName = 'StudentGrades';
    const url = `mongodb://localhost:27017/${dbName}`;
    const collectionName = 'studentgrades';

    const studentSchema = new mongoose.Schema({
        studentID: { type: String, required: true },
        courseID: { type: String, required: true },
        rollno: { type: String, required: true },
        email: { type: String, required: true },
        grade: { type: String, required: true },
    });
    studentSchema.index({ studentID: 1, courseID: 1 }); 

    const csvFilePath = './student_course_grades.csv';

    const columnMapping = { // column in csv file to field in MongoDB
        studentID: 'student-ID',
        courseID: 'course-id',
        rollno: 'roll no',
        email: 'email ID',
        grade: 'grade',
    };

    loadData({
        url,
        collectionName,
        schema: studentSchema,
        csvFilePath,
        columnMapping
    }).catch((error) => {
        console.error(`Error loading data: ${error.message}`);
        mongoose.connection.close();
    });
}

module.exports = { connectMongo, loadData };
