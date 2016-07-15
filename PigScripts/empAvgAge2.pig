employees = LOAD '/user/cloudera/employees' USING PigStorage(',') AS (emp_no:int, birth_date:datetime, dept_no:chararray);
departments = LOAD '/user/cloudera/departments' USING PigStorage(',') AS (dept_no:chararray, dept_name:chararray);


empAge = FOREACH employees GENERATE dept_no, emp_no, YearsBetween(CurrentTime(), birth_date) AS age;

-- deptDataGrp = GROUP deptData BY dept_no;
deptDataGrp = GROUP empAge BY dept_no;
deptAge = FOREACH deptDataGrp GENERATE group, AVG(empAge.age) as avg;

final = FOREACH (JOIN deptAge BY group, departments BY dept_no) GENERATE departments::dept_name, deptAge::avg;


STORE final INTO '/user/clouderea/dept_avg_age';

-- DUMP final;
-- DESCRIBE deptData;
-- DUMP empAge;
-- DESCRIBE empAge;
-- STORE empAge INTO 'dept_data';