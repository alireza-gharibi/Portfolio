# Data Analysis using Spark

## Scenario
I have been tasked by the HR department of a company to create a data pipeline that can take in employee data in a CSV format. My responsibilities include analyzing the data, applying any required transformations, and facilitating the extraction of valuable insights from the processed data. \
Given my role as a data engineer, i've been requested to leverage Apache Spark components to accomplish the tasks.
[Project Code](https://github.com/alireza-gharibi/Portfolio/blob/main/Spark/Saprk%202/spark%202%20project.py)
## preparing the environment:
- I start a **spark** container and run **pyspark** in that container:
```
docker run --rm -it spark:python3 /opt/spark/bin/pyspark
```
- make sure you have **java** installed on your system.

## Project Overview
I create a DataFrame by loading data from a CSV file and apply transformations and actions using **SparkSQL**. This needs to be achieved by performing the following tasks:
- Task 1: Generate DataFrame from CSV data.
- Task 2: Define a schema for the data.
- Task 3: Display schema of DataFrame.
- Task 4: Create a temporary view.
- Task 5: Execute an SQL query.
- Task 6: Calculate Average Salary by Department.
- Task 7: Filter and Display IT Department Employees.
- Task 8: Add 10% Bonus to Salaries.
- Task 9: Find Maximum Salary by Age.
- Task 10: Self-Join on Employee Data.
- Task 11: Calculate Average Employee Age.
- Task 12: Calculate Total Salary by Department.
- Task 13: Sort Data by Age and Salary.
- Task 14: Count Employees in Each Department.
- Task 15: Filter Employees with the letter 'o' in the Name.




