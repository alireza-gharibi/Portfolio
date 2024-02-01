## ETL top 10 banks by market capitalization
### Project Scenario: 
I have been hired as a data engineer by research organization. my boss has asked me to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to me as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table. \
[Python Code](https://github.com/alireza-gharibi/Portfolio/blob/main/ETL_World's%20Largest%20Banks_Project/ETL%20World's%20Largest%20Banks%20Project.ipynb)
[Data URL](https://web.archive.org/web/20230908091635)   
[Exchange rate CSV](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv) 

### Tasks:
- Extract the tabular information from the given URL under the heading By Market Capitalization, and save it to a dataframe.
- Transform the dataframe by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
- Load the transformed dataframe to an output CSV file.
- Load the transformed dataframe to an SQL database server as a table.
- Run queries on the database table.
- Log the progress of the code.
