
    student_data = LOAD './student_course_grades.csv' USING PigStorage(',') AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);

    STORE student_data INTO '/student_data' USING PigStorage(',');
  