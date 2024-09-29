WITH ranked_employee_data AS (
  SELECT 
    employe_id, 
    branch_id, 
    salary,
    join_date,
    resign_date,
    RANK() OVER (PARTITION BY employe_id, branch_id, join_date, resign_date ORDER BY salary DESC) AS ranked_value -- will take record which have max salary from duplicate records
  FROM employees
), employee_data_cleaned AS (
  SELECT 
    employe_id AS employee_id,
    branch_id, 
    salary,
    join_date,
    resign_date
  FROM ranked_employee_data
  WHERE ranked_value = 1
), ranked_timesheet_data AS (
  SELECT
    timesheet_id,
    employee_id,
    date,
    checkin,
    checkout,
    RANK() OVER (PARTITION BY employee_id, date ORDER BY timesheet_id ASC) AS ranked_value -- first record in duplicate data that have complete checkin and checkout value
  FROM timesheets
), timesheet_data_cleaned AS (
  SELECT
    timesheet_id,
    employee_id,
    date,
    checkin,
    checkout
  FROM ranked_timesheet_data
  WHERE ranked_value = 1
), employee_timesheet_data AS (
  SELECT
    t.timesheet_id,
    t.employee_id,
    e.branch_id,
    e.salary,
    e.join_date,
    e.resign_date,
    t.date,
    t.checkin,
    t.checkout
  FROM timesheet_data_cleaned t
  LEFT JOIN employee_data_cleaned e ON e.employee_id = t.employee_id
  WHERE t.date <= e.resign_date OR e.resign_date IS NULL -- invalid data, because employee checkin after resignation date
), employee_work_hour AS (
  SELECT 
    employee_id,
    branch_id,
    date,
    CASE
      WHEN checkin > checkout THEN 0 -- invalid data but still need to count the day
      ELSE COALESCE(TIME_DIFF(checkout, checkin, SECOND) / 3600, 0) -- take the substract value of checkout and checkin in seconds and convert it into hour
    END AS work_hour,
  FROM employee_timesheet_data
), total_work_hour_per_branch AS (
  SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    branch_id,
    SUM(work_hour) AS total_work_hour
  FROM employee_work_hour
  GROUP BY 1,2,3
), salary_per_employee_branch AS (
  SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    branch_id,
    employee_id,
    MAX(salary) AS salary_per_month
  FROM employee_timesheet_data
  GROUP BY 1,2,3,4
), total_salary_per_branch AS (
  SELECT
    year,
    month,
    branch_id,
    SUM(salary_per_month) AS total_salary
  FROM salary_per_employee_branch
  GROUP BY 1,2,3
)
SELECT
  twhpb.year,
  twhpb.month,
  twhpb.branch_id,
  CASE
    WHEN twhpb.total_work_hour = 0 THEN 0 -- If work hour = 0 cause there's some issue cause by missing checkin/checkout, checkin = checkout, checkin > checkout
    ELSE ROUND(tspb.total_salary / twhpb.total_work_hour, 2)
  END AS salary_per_hour
FROM total_work_hour_per_branch twhpb
LEFT JOIN total_salary_per_branch tspb 
  ON 
    twhpb.year = tspb.year 
    AND twhpb.month = tspb.month 
    AND twhpb.branch_id = tspb.branch_id
ORDER BY year, month, branch_id ASC