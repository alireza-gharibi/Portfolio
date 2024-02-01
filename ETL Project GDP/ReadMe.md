## Project Scenario
An international firm that is looking to expand its business in different countries across the world has recruited me. I have been hired as a junior Data Engineer and am tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.

### Objectives:
The required information needs to be made accessible as a JSON file **Countries_by_GDP.json** as well as a table **Countries_by_GDP** in a database file **World_Economies.db** with attributes **Country** and **GDP_USD_billion**.
My boss wants me to demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy.   
Also, log the entire process of execution in a file named **etl_project_log.txt**.

I must create a Python code **etl_project_gdp.py** that performs all the required tasks.
 
 ### Tasks: 

- Write a data extraction function to retrieve the relevant information from the required URL.
- Transform the available GDP information into 'Billion USD' from 'Million USD'.
- Load the transformed information to the required CSV file and as a database file.
- Run the required query on the database.
- Log the progress of the code with appropriate timestamps
