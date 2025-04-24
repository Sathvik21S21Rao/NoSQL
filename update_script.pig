
            student_data = LOAD 'student_course_grades.csv' USING PigStorage(',') 
                AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);

            student_data_selected = FILTER student_data BY
                student_id == 'SID1033' AND course_id == 'CSE016';

            DUMP student_data_selected;
        