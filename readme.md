# NoSQL Database Synchronization Project

## Introduction

This project demonstrates a synchronization mechanism between three different database systems: MongoDB, PostgreSQL, and Apache Pig. The goal is to maintain consistency across these systems by logging operations (oplogs) and merging changes when required. The project supports operations like setting, getting, and merging data across the databases, ensuring that updates are propagated correctly.

## Setup

1. **Install Dependencies**:
   Ensure you have Node.js installed. Run the following command to install the required dependencies:
   ```bash
   npm install
   ```

2. **Environment Setup**:
   - MongoDB: Ensure MongoDB is running locally on `localhost:27017`.
   - PostgreSQL: Ensure PostgreSQL is running locally and accessible with the credentials provided in the code.
   - Apache Pig: Ensure Apache Pig is installed and configured correctly with access to HDFS.

3. **CSV File**:
   Place the `student_course_grades.csv` file in the root directory. This file will be used to initialize the databases.

4. **Database Initialization**:
   - Run the scripts in the `insertion_creation` folder to initialize the databases:
     - `mongoSetup.js` for MongoDB
     - `insert_psql.js` for PostgreSQL
     - `insert_pig.js` for Apache Pig

5. **Reset Logs**:
   Use the `reset.js` script to reset the oplogs and synchronization timestamps:
   ```bash
   node insertion_creation/reset.js
   ```

## Oplogs

Oplogs (operation logs) are used to track changes made to each database. These logs are stored in the `OpLogs` directory:
- `opLog_MongoDB.jsonl`: Logs for MongoDB operations.
- `opLog_Postgres.jsonl`: Logs for PostgreSQL operations.
- `opLog_Pig.jsonl`: Logs for Apache Pig operations.

Each log entry contains:
- `collection`: The collection or table name.
- `field`: The field being modified.
- `timestamp`: The time of the operation.
- `type`: The type of operation (e.g., `update`, `insert`).
- `data`: The data involved in the operation.

## `main2.js`

The `main2.js` file is the entry point of the project. It reads commands from the `testcase.in` file and executes them. Supported commands include:
- `MONGO.SET

# Project Structure and File Execution Order

## Project Structure

```
NoSQL/
├── insertion_creation/
│   ├── mongoSetup.js         # Initializes MongoDB with data from the CSV file.
│   ├── insert_psql.js        # Inserts data into PostgreSQL from the CSV file.
│   ├── insert_pig.js         # Loads data into Apache Pig from the CSV file.
│   ├── reset.js              # Resets oplogs and synchronization timestamps.
├── main2.js                  # Entry point for executing commands from testcase.in.
├── testcases/
│   ├── testcase.in           # Input file containing commands for testing.
│   ├── testcase.out          # Expected output for the test cases.
├── OpLogs/
│   ├── opLog_MongoDB.jsonl   # Operation log for MongoDB.
│   ├── opLog_Postgres.jsonl  # Operation log for PostgreSQL.
│   ├── opLog_Pig.jsonl       # Operation log for Apache Pig.
├── student_course_grades.csv # CSV file used to initialize the databases.
├── package.json              # Node.js dependencies and scripts.
└── readme.md                 # Documentation for the project.
```

## File Execution Order

1. **Database Initialization**:
   - Run the following scripts to initialize the databases:
     ```bash
     node insertion_creation/mongoSetup.js
     node insertion_creation/insert_psql.js
     node insertion_creation/insert_pig.js
     ```

2. **Reset Logs**:
   - Reset the oplogs and synchronization timestamps:
     ```bash
     node insertion_creation/reset.js
     ```

3. **Run the Main Program**:
   - Execute the `main2.js` file to process commands from `testcase.in`:
     ```bash
     node main2.js
     ```

4. **Test Cases**:
   - Use the `testcase.in` file to test the synchronization and operations. Compare the output with `testcase.out` for validation.
```