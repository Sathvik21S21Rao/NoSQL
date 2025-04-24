raw_data = LOAD './studentgrades' 
    USING PigStorage(',') 
    AS (student_id:chararray, course_id:chararray, roll_no:chararray, email_id:chararray, grade:chararray);

STORE raw_data INTO '/student_data' USING PigStorage(',');
