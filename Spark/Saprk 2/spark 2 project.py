
# Project: Data Analysis using Spark

# - Task 1: Generate DataFrame from CSV data.
# - Task 2: Define a schema for the data.
# - Task 3: Display schema of DataFrame.
# - Task 4: Create a temporary view.
# - Task 5: Execute an SQL query.
# - Task 6: Calculate Average Salary by Department.
# - Task 7: Filter and Display IT Department Employees.
# - Task 8: Add 10% Bonus to Salaries.
# - Task 9: Find Maximum Salary by Age.
# - Task 10: Self-Join on Employee Data.
# - Task 11: Calculate Average Employee Age.
# - Task 12: Calculate Total Salary by Department.
# - Task 13: Sort Data by Age and Salary.
# - Task 14: Count Employees in Each Department.
# - Task 15: Filter Employees with the letter o in the Name.


# Installing required packages
#%pip install pyspark findspark wget


import findspark
findspark.init()


# importing modules: 
import wget
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField, DateType
from pyspark.sql.functions import avg
from pyspark.sql.functions import sum
from pyspark.sql.functions import round
from pyspark.sql.functions import max
from pyspark.sql.functions import count
from pyspark.sql.functions import col, asc, desc


# Creating a SparkContext object  
sc = SparkContext.getOrCreate()

# Creating a SparkSession  
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


# Download the CSV data first into a local `employees.csv` file
wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")



#### Task 1: Generate a Spark DataFrame from the CSV data
# Read data from the "emp" CSV file and import it into a DataFrame variable named "employees_df"  
employees_df = spark.read.csv( 'employees.csv' , sep = ',' , inferSchema = True , header = True )


# #### Task 2: Define a schema for the data  
# Defining a Schema for the input data and read the file using the user-defined Schema. csv columns: Emp_No, Emp_Name, Salary, Age, Department
schema = StructType([
    StructField("Emp_No", IntegerType()),
    StructField("Emp_Name", StringType()),
    StructField("Salary", IntegerType()),
    StructField("Age", IntegerType()),
    StructField("Department", StringType())
])

employees_df = (spark.read
  .format("csv")
  .schema(schema)
  .option("header", "true")
  .load("employees.csv")
)



#### Task 3: Display schema of DataFrame
employees_df.printSchema()


#### Task 4: Create a temporary view
# Create a temporary view named "employees" for the DataFrame
employees_df.createTempView("employees")


#### Task 5: Execute an SQL query
# SQL query to fetch solely the records from the View where the age exceeds 30
spark.sql("select * from employees where Age > 30 ").show()


#### Task 6: Calculate Average Salary by Department
# SQL query to calculate the average salary of employees grouped by department
spark.sql("select Department, avg(Salary) from employees group by Department").show()


#### Task 7: Filter and Display IT Department Employees
# Apply a filter to select records where the department is 'IT'
employees_df.where(employees_df['Department'] == 'IT').show()      # also we can use:   employees_df.filter(employees_df['Department'] == 'IT').show()



#### Task 8: Add 10% Bonus to Salaries
# Add a new column "SalaryAfterBonus" with 10% bonus added to the original salary
employees_df = employees_df.withColumn("SalaryAfterBonus", round(employees_df.Salary*1.10) )  # salary + 10% salary = 1.10 salary
employees_df.show()


#### Task 9: Find Maximum Salary by Age
# Group data by age and calculate the maximum salary for each age group
employees_df.groupby('Age').max('Salary').show()


#### Task 10: Self-Join on Employee Data
# Join the DataFrame with itself based on the "Emp_No" column
employees_df.join(employees_df , on = 'Emp_No' , how = 'inner' ).show()


#### Task 11: Calculate Average Employee Age
# Calculate the average age of employees
employees_df.agg(avg('Age')).show()


#### Task 12: Calculate Total Salary by Department
# Calculate the total salary for each department. 
employees_df.groupby('Department').agg(sum('Salary').alias('Total_Salary_per_Department')).show()


#### Task 13: Sort Data by Age and Salary
# Sort the DataFrame by age in ascending order and then by salary in descending order
employees_df.sort(col("Age").asc(), col("Salary").desc()).show()   # we can also use orderBy instead of sort 


#### Task 14: Count Employees in Each Department
# Calculate the number of employees in each department
employees_df.groupby('Department').count().show()


#### Task 15: Filter Employees with the letter o in the Name
# Apply a filter to select records where the employee's name contains the letter 'o'
employees_df.filter(col("Emp_Name").contains('o')).show()