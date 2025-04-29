
            student_data = LOAD 'studentgrades' USING PigStorage(',') 
                AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);
        
             student_data = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1081' AND course_id == 'CSE016' ? 'C' : grade) AS grade;
        
             student_data = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1081' AND course_id == 'CSE016' ? 'D' : grade) AS grade;
        
             student_data = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1081' AND course_id == 'CSE016' ? 'B' : grade) AS grade;
        
            STORE student_data INTO '/user/hadoop1/temp' USING PigStorage(',');
        