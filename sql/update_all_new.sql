--
-- https://company.atlassian.net/browse/PIV15-1699
--
-- To use remotely with Docker:  mysql -uroot -h172.17.0.2 < ~/company/tasks/PIV15-1699/update_istep_new.sql
-- 
--  mysql -uroot -h172.17.0.2 < update_all_new.sql 
--
-- run in container:  php public/index.php queue sqs pivot_sync
-- USE pivot_develop;

UPDATE dw_tests SET enabled = 1 WHERE test_key IN ('mstep', 'wida','raps360', 'iread', 'istep', 'acuity');
UPDATE tests SET test_type = 'new', enabled =1 WHERE test_filename IN ('mstep', 'wida','raps360', 'iread', 'istep','acuity');
