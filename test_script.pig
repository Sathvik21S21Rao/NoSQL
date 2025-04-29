student_data = LOAD 'studentgrades' USING PigStorage(',') 
                AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);

student_data_selected = FILTER student_data BY
    student_id == 'SID1001';

DUMP student_data_selected;