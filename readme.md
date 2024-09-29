# Introduction

This project is for calculating salary per hour using Python and SQL scripts. Below is an overview of the folder structure.

# Folder Structure
```bash
.
├── data/ 
│ └── employees.csv # CSV files for employees data 
│ └── timesheets.csv # CSV files for timesheets data 
├── python_scripts/ 
│ └── etl_daily_calculate_salary_per_hour_per_branch.py # Python script to calculate salary per hour and incrementally load data daily to table 
├── sql_scripts/ 
│ ├── load_csv_to_table.sql # SQL script to create schema and load data to tables
│ └── etl_calculate_salary_per_hour_per_branch.sql # SQL script to calculate salary per hour and overwrite data to table
└── .env.example # Environment file to save Database credentials
```

# Details
- Remove duplicate data in employees data using rank data based on max salary that groupped by employe_id, branch_id, join_date and resign_date
- Remove duplicate data in timesheets data using rank data based on min timesheet_id that groupped by employee_id and date
- Remove invalid data which is have timesheets after resignation date
- When calculating work hour, invalid timesheet data that have checkin > checkout, set as 0, to keep it counted. For the valid data, get the different between checkout and checkin value in seconds format, and the result will be divided with 3600 to make it into hour format
- When calculating salary per hour, records that have total work hour 0, set as 0 to keep it counted and prevent division by zero