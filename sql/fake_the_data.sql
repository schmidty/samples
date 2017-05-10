--
-- HOW TO "FAKE THE DATA"; The Brittany Way
--

-- First import an istep file the old way
-- and import a wida file with POSTMAN
-- 
-- Next add WIDA to the `tests` table,
-- dw_test_id should be 32 and test_filename should be lower case 'wida' and test_type is 'new'
--
-- Go to you `test_istep` table
-- Find the `psid` column.
SET @ordering_inc = 1;
SET @new_ordering = 0;
UPDATE ua_student_year_grade SET student_id = (@new_ordering := @new_ordering + @ordering_inc);

-- make sure the `ua_student_year_grade`
-- You have to find the specific `psid` in the old Pivot old front-end
SELECT a.psid, b.student_id, b.id, a.last_name, a.first_name, a.dob FROM pivot_students a JOIN ua_student_year_grade b ON a.psid = b.student_id;

-- Next hit the "View Longitudinal Data" after checking off the students from the last query
-- Also check the "test" tab
