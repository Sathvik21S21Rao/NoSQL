
            student_data = LOAD 'studentgrades' USING PigStorage(',') 
                AS (student_id:chararray, course_id:chararray, roll_no:chararray, email:chararray, grade:chararray);
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1002' AND course_id == 'CSE101' ? 'C' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1001' AND course_id == 'CSE101' ? 'A-' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1002' AND course_id == 'CSE101' ? 'C+' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1002' AND course_id == 'CSE101' ? 'C+' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1002' AND course_id == 'CSE101' ? 'C+' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1002' AND course_id == 'CSE101' ? 'A-' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1002' AND course_id == 'CSE101' ? 'C' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1001' AND course_id == 'CSE101' ? 'A-' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1002' AND course_id == 'CSE101' ? 'A-' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1033' AND course_id == 'CSE016' ? 'F' : grade) AS grade;
        
             student_data_new = FOREACH student_data GENERATE
                    student_id,
                    course_id,
                    roll_no,
                    email,
                    (student_id == 'SID1033' AND course_id == 'CSE016' ? 'F' : grade) AS grade;
        
            STORE student_data_new INTO '/user/hadoop1/temp' USING PigStorage(',');
        