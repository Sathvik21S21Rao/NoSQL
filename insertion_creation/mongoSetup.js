mongoose = require('mongoose');
mongoose.set('strictQuery', false); // to avoid deprecation warning
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
    }

const studentSchema = new mongoose.Schema({
    studentID: {
        type: String,
        required: true,
    },
    courseID: {
        type: String,
        required: true,
    },
    rollno: {
        type: String,
        required: true,
    },
    email: {
        type: String,
        required: true,
    },
    grade: {
        type: String,
        required: true,
    },
});

studentSchema.index({ studentID: 1, courseID: 1 });
async function loadData(url){
    await connectMongo(url);
    // if collection already exists, drop it
    const collectionName = 'studentgrades';
    const collection = mongoose.connection.collection(collectionName);
    const collectionExists = await collection.findOne({});
    if (collectionExists) {
        await collection.drop();
        console.log(`Collection ${collectionName} dropped`);
    } else {
        console.log(`Collection ${collectionName} does not exist`);
    }
    const Student = mongoose.model(collectionName, studentSchema);

    const fs = require('fs');
    const csv = require('csv-parser');
    const csvFilePath = './student_course_grades.csv';
    const csvData = [];
    fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (row) => {
            modified_row={
                studentID: row["student-ID"],
                courseID: row['course-id'],
                rollno: row['roll no'],
                email: row['email ID'],
                grade: row['grade'],
            }
            csvData.push(modified_row);
        })
        .on('end', async () => {
            console.log('CSV file successfully processed');
            await Student.insertMany(csvData);
            console.log('Data inserted successfully');
            mongoose.connection.close();
        });
}

if (require.main === module) {
    const mongoose = require('mongoose');
    const dbName='StudentGrades';
    const url=`mongodb://localhost:27017/${dbName}`;
    loadData(url).catch((error) => {
        console.error(`Error loading data: ${error.message}`);
        mongoose.connection.close();
    });
}



// export { connectMongo };
