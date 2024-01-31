
# Installing required packages:
#!pip install wget pyspark findspark


import findspark
findspark.init()

# importing libraries

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField, DateType
from pyspark.sql.functions import year, quarter, to_date
from pyspark.sql.functions import avg
from pyspark.sql.functions import sum
from pyspark.sql.functions import when
from pyspark.sql.functions import lit


# Creating a SparkContext object
sc = SparkContext.getOrCreate()

# Creating a Spark Session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .enableHiveSupport() \
    .getOrCreate()


### Task 1: Load datasets into PySpark DataFrames
#download dataset using wget
import wget

link_to_data1 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset1.csv'
wget.download(link_to_data1)

link_to_data2 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset2.csv'
wget.download(link_to_data2)

#load the data into a pyspark dataframe
df1 = spark.read.csv("dataset1.csv", header=True, inferSchema=True)  # automatically infers the schema 
df2 = spark.read.csv("dataset2.csv", header=True, inferSchema=True)


#print the schema of df1 and df2
df1.printSchema()
df2.printSchema()


# Add a new column named **year** to `df1` and **quarter** to `df2` representing the year and quarter of the data.

df1 = df1.withColumn("date_column", to_date(df1.date_column,"d/M/y")) #converting 'date_column' from "string" type to "date" type
df1 = df1.withColumn('year', year(df1.date_column))  #Add new column 'year' to df1

df2 = df2.withColumn("transaction_date", to_date(df2.transaction_date,"d/M/y")) #converting 'date_column' from "string" type to "date" type:
df2 = df2.withColumn('quarter', quarter(df2.transaction_date)) #Add new column 'quarter' to df2


# Rename the column **amount** to **transaction_amount** in `df1` and **value** to **transaction_value** in `df2`:

df1 = df1.withColumnRenamed('amount', 'transaction_amount') #Rename df1 column amount to transaction_amount

df2 = df2.withColumnRenamed('value', 'transaction_value') #Rename df2 column value to transaction_value


#### Task 5: Drop unnecessary columns

df1 = df1.drop('description', 'location') #Drop columns description and location from df1

df2 = df2.drop('notes') #Drop column notes from df2


#### Task 6: Join dataframes based on a common column
#i join df1 and df2 based on common column customer_id:
joined_df = df1.join(df2, on = 'customer_id', how = 'inner')  # or joined_df = df1.join(df2, df1.customer_id == df2.customer_id, 'inner')

#### Task 7: Filter `joined_df` to include only transactions where "transaction_amount" is greater than 1000 and create a new dataframe named `filtered_df`.
filtered_df = joined_df.where( 'transaction_amount > 1000' )  # make sure to enclose the whole condition in  quotes ''


#### Task 8: Aggregate data by customer
# Calculate the total transaction amount for each customer in `filtered_df` and display the result.
total_amount_per_customer = filtered_df.groupby('customer_id').sum('transaction_amount')
total_amount_per_customer.show() #displaying the result


#### Task 9: Write the result to a Hive table
total_amount_per_customer.write.mode("overwrite").saveAsTable("customer_totals")


#### Task 10: Write the filtered data to HDFS
# Write `filtered_df` to HDFS in parquet format to a file named **filtered_data**.
filtered_df.write.mode("overwrite").parquet("filtered_data.parquet")


#### Task 11: Add a new column based on a condition
# Add new column with value indicating whether transaction amount is > 5000 or not
df1.withColumn("high_value", \
   when((df1.transaction_amount > 5000), lit("Yes")).otherwise(lit("No")))


#### Task 12: Calculate the average transaction value per quarter
#calculate the average transaction value for each quarter in df2
average_value_per_quarter = df2.groupBy('quarter').agg(avg("transaction_value").alias("avg_trans_val"))
average_value_per_quarter.show() #showing the average transaction value for each quarter in df2  


#### Task 13: Write the result to a Hive table

average_value_per_quarter.write.mode("overwrite").saveAsTable("quarterly_averages") # Writing `average_value_per_quarter` to a Hive table named **quarterly_averages**.


#### Task 14: Calculate the total transaction value per year
# calculate the total transaction value for each year in df1.
total_value_per_year = df1.groupby('year').agg(sum('transaction_amount').alias('total_transaction_val'))
total_value_per_year.show() # showing the total transaction value for each year in df1.


#### Task 15: Write the result to HDFS
#Write total_value_per_year to HDFS in the CSV format
total_value_per_year.write.mode("overwrite").csv("total_value_per_year.csv")

