-- Create schema for employees table and replace the table if already exists
CREATE OR REPLACE TABLE employees (
    employe_id VARCHAR PRIMARY KEY,
    branch_id VARCHAR NOT NULL,
    salary INT NOT NULL,
    join_date DATE NOT NULL,
    resign_date TIME NULL
);

-- Load data for timesheets table from '../data/employee.csv' directory
COPY employees (employe_id, branch_id, salary, join_date, resign_date)
FROM '../data/timesheets.csv'
DELIMITER ','
CSV HEADER;

-- Create schema for timesheets table and replace the table if already exists
CREATE OR REPLACE TABLE timesheets (
    timesheet_id VARCHAR PRIMARY KEY,
    employee_id VARCHAR NOT NULL,
    date DATE NOT NULL,
    checkin TIME NULL,
    checkout TIME NULL,
    FOREIGN KEY (employee_id) REFERENCES employees (employe_id)
);

-- Load data for timesheets table from '../data/timesheets.csv' directory
COPY timesheets (timesheet_id, employee_id, date, checkin, checkout)
FROM '../data/timesheets.csv'
DELIMITER ','
CSV HEADER;