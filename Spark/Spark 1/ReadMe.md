## Scenario 
I have been hired as a Junior Data Engineer by BDPS Corporation and i have been provided with links to two raw datasets that i need to acquire and perform ETL on using **PySpark** and **Hive** warehouse. \
[Project Code](https://github.com/alireza-gharibi/Portfolio/blob/main/Spark/Spark%201/Spark%201.py)
## preparing the environment:
- I start a **spark** container and run **pyspark** in that container:
```
docker run --rm -it spark:python3 /opt/spark/bin/pyspark
```
- make sure you have **java** installed on your system.

### Project Overview
This project focuses on data transformation and integration using PySpark. I will work with two datasets, and perform various transformations such as **adding columns, renaming columns, dropping unnecessary columns, joining dataframes,** and finally, writing the results into a **Hive** warehouse and an **HDFS** file system.
