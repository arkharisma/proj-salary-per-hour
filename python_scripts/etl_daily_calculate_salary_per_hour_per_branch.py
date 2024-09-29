import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Union, List
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

class TimesheetEmployeeProcessor:
    def __init__(self, df: pd.DataFrame = None) -> None:
        if df is None:
            # If no existing dataframe to be processed, set to None
            self.data = None
        else:
            # If there's existing dataframe to be processed, copy the dataframe
            self.data = df.copy()
        
    def load_data_from_csv(self, pathfile: str, delimiter: str = ','):
        """Load data from CSV and set to `data` attribute

        Args:
            pathfile: Pathfile for CSV file.
            delimiter: Delimiter for reading CSV file.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        self.data = pd.read_csv(pathfile, delimiter=delimiter)
        return self

    def remove_duplicate_data(self, partitioning_keys: Union[str, List[str]], ordering_key: str, ascending_order: bool = True):
        """Remove duplicate data using rank that partitioned by `patitioning_keys`
        and ordered by `ordering_key`

        Args:
            partitioning_keys: Key for partitioning rank data. E.g. 'field1' or ['field1', 'field2'].
            ordering_key: Key for ordering rank data. E.g. 'field1;.
            ascending_order: Ranking data using ascending ordering.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        idx = self.data.groupby(partitioning_keys)[ordering_key]
        
        if ascending_order: idx = idx.idxmin()
        else: idx = idx.idxmax()
        
        # Set data based on the previous index
        self.data =  self.data.loc[idx].reset_index(drop=True)
        return self
    
    def filter_timesheets_by_date(self, date:str):
        """Filtering data by `date` param

        Args:
            date: Date value for filtering data. E.g. '2020-01-01'.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        self.data = self.data.loc[self.data['date'] == date]
        return self
    
    def get_data(self) -> pd.DataFrame:
        """Retrieve the current data stored in the instance.

        Returns:
            pd.DataFrame: THe current data stored in the instance.
        """
        return self.data
    
    def filter_valid_data(self):
        """Filtering for only valid data.
        
        This method filtering invalid data which have `date` value greater than `resign_date`.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        # Change date and resign_date into datetime
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['resign_date'] = pd.to_datetime(self.data['resign_date'])

        # Filter only valid data, timesheet date <= resign_date and resign_date is null
        self.data = self.data[
            (self.data['date'] <= self.data['resign_date']) | 
            self.data['resign_date'].isna()
        ]
        return self
    
    def select_fields(self, fields: list[str]):
        """Selecting `fields` in stored data.

        Args:
            fields: List of field that want to be selected. E.g. ['field1', 'field2', 'field3'].

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        self.data = self.data[fields]
        return self
    
    def calculate_work_hour(self):
        """Calculating work hour for every timesheet data.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        # Convert checkin and checkout data into timedelta
        self.data['checkin'] = pd.to_timedelta(self.data['checkin'])
        self.data['checkout'] = pd.to_timedelta(self.data['checkout'])

        # Set work_hour to 0 if checkin greated that checkout
        # If checkin <= checkout, substract checkout and checkin in seconds value and the result will be convert into hour value
        self.data['work_hour'] = np.where(
            self.data['checkin'] > self.data['checkout'], 0,
            (self.data['checkout'] - self.data['checkin']).dt.total_seconds() / 3600
        )
        # Set null value with 0
        self.data['work_hour'] = self.data['work_hour'].fillna(0)
        return self

    def sum_work_hour(self):
        """Calculating total work hour groupped by year, month and branch_id.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        # Convert date value into datetime and extract year and month value from date
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['year'] = self.data['date'].dt.year
        self.data['month'] = self.data['date'].dt.month

        # Sum the work hour grouped by year, month, and branch_id
        self.data = self.data\
            .groupby(['year', 'month', 'branch_id'], as_index=False)['work_hour']\
            .sum()

        # Rename the aggregated work_hour column
        self.data.rename(columns={'work_hour': 'total_work_hour'}, inplace=True)
        return self
    
    def get_salary_per_employee(self):
        """Get max salary per employee groupped by year, month, branch_id and employee_id.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        # Convert date value into datetime and extract year and month value from date
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.data['year'] = self.data['date'].dt.year
        self.data['month'] = self.data['date'].dt.month

        # Sum salary grouped by year, month, branch_id and employee_id
        self.data = self.data\
            .groupby(['year', 'month', 'branch_id', 'employee_id'], as_index=False)['salary']\
            .max()
            
        # Rename aggregated salary column
        self.data.rename(columns={'salary': 'salary_per_month'}, inplace=True)
        return self
    
    def sum_salary_per_branch(self):
        """Calculating total salary groupped by year, month and branch_id.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        # Sum salary_per_month grouped by year, month and branch_id
        self.data = self.data.groupby(['year', 'month', 'branch_id'], as_index=False)['salary_per_month'].sum()

        # Rename aggregated salary_per_month column
        self.data.rename(columns={'salary_per_month': 'total_salary'}, inplace=True)
        return self

    def calculate_salary_per_hour(self):
        """Calculating salary per hour for each branch. Groupped by year, month and branch_id.
        If total_work_hour in a branch = 0, than salary per hour is 0.
        Result of dividing total salary and total work hour will be rounded to 2 decimal.

        Returns:
            TimesheetEmployeeProcessor: The instance of the DataProcessor (self), allowing for method chaining.
        """
        # Set salary per hour to 0 if work hour is 0,
        # if salary is not 0, divide total salary and total work hour and round it into 2 decimal
        self.data['salary_per_hour'] = np.where(
            self.data['total_work_hour'] == 0, 0,  # If condition is true (invalid data)
            np.round((self.data['total_salary'] / self.data['total_work_hour']), 2)
        )
        return self

# Employee and Timesheet CSV Pathfile
employee_pathfile = '../data/employees.csv'
timesheet_pathfile = '../data/timesheets.csv'

# Retrieve and clean employee data
employee_data = TimesheetEmployeeProcessor()\
    .load_data_from_csv(employee_pathfile)\
    .remove_duplicate_data(partitioning_keys=['employe_id', 'branch_id'], ordering_key='salary', ascending_order=False)\
    .get_data()

# Retrieve and clean timesheet data
timesheet_data = TimesheetEmployeeProcessor()\
    .load_data_from_csv(timesheet_pathfile)\
    .filter_timesheets_by_date((datetime.today() - timedelta(days=1)).date())\
    .remove_duplicate_data(partitioning_keys=['employee_id', 'date'], ordering_key='timesheet_id')\
    .get_data()

# Join timesheet data with employee data
joined_employee_timesheet_data = pd.merge(timesheet_data, employee_data, left_on='employee_id', right_on='employe_id')

# Clean data after being joined and select only necessary fields
employee_timesheet_data = TimesheetEmployeeProcessor(joined_employee_timesheet_data)\
    .filter_valid_data()\
    .select_fields(['timesheet_id', 'employee_id', 'branch_id', 'salary', 'join_date', 'resign_date', 'date', 'checkin', 'checkout'])\
    .get_data()

# Calculate work hour data
work_hour_data = TimesheetEmployeeProcessor(employee_timesheet_data)\
    .calculate_work_hour()\
    .sum_work_hour()\
    .get_data()
    
# Calculate salary data
salary_data = TimesheetEmployeeProcessor(employee_timesheet_data)\
    .get_salary_per_employee()\
    .sum_salary_per_branch()\
    .get_data()

# Join work hour per branch and salary per branch data
joined_work_hour_salary_data = pd.merge(work_hour_data, salary_data, on=['year', 'month', 'branch_id'])

# Calculate salary per hour for each branch
salary_per_hour_data = TimesheetEmployeeProcessor(joined_work_hour_salary_data)\
    .calculate_salary_per_hour()\
    .select_fields(['year', 'month', 'branch_id', 'salary_per_hour'])\
    .get_data()
    
# Loading data into table
def generate_increment_data_query(records: pd.DataFrame) -> str:
    """Generate query for increment data in branch_hourly_salary table.

    Args:
        records: Salary per hour dataframe.

    Returns:
        str: Query string for increment data in branch_hourly_salary table.
    """
    query = "INSERT INTO branch_hourly_salary (year, month, branch_id, salary_per_hour) VALUES "
    data_values = []
    for _, data in records.iterrows():
        data_values.append(f"({int(data['year'])}, {int(data['month'])}, {int(data['branch_id'])}, {data['salary_per_hour']})")
    query += ', '.join(data_values)
    return query

increment_data_query = generate_increment_data_query(salary_per_hour_data)

try:
    load_dotenv()
    db_engine = create_engine('postgresql+psycopg2://' + 
                            os.getenv('DB_USERNAME') +':' + 
                            os.getenv('DB_PASSWORD') + '@' +
                            os.getenv('DB_HOST') + ':' +
                            os.getenv('DB_PORT') + '/' +
                            os.getenv('DB_NAME'));
    db_connection = db_engine.connect()
    db_connection.execute(increment_data_query)
except SQLAlchemyError as err:
    print('Error', err.__cause__)