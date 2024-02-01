## Scenario
I am a data engineer at a data analytics consulting company. I have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. My job is to collect data available in different formats and consolidate it into a single file.
File is available at [URL](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz)

When you extract the zip file you should see the following 3 files.

File list:
- vehicle-data.csv
- tollplaza-data.tsv
- payment-data.txt

### what i'm assigned to do:
- downlaod and unzip the data
- create tasks to extract csv,tsv and fixed width files.
- Create a task to consolidate data extracted from files (csv_data.csv, tsv_data.csv, fixed_width_data.csv)
- transform the vehicle_type field in extracted_data.csv into capital letters and load(save) it into a file named transformed_data.csv in the staging directory.
- The final csv file should use the fields in the order given below:

**Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, Tollplaza id, Tollplaza code, Type of Payment code, and Vehicle Code**

### requirements to run the project file: 
- dos2unix 
- airflow

dos2unix can be installed via:
```
sudo apt-get update
sudo apt-get install dos2unix
```

### vehicle-data.csv:
vehicle-data.csv is a **comma-separated values** file.

It has the 6 below fields:

- Rowid  - This uniquely identifies each row. This is consistent across all the three files.
- Timestamp - What time did the vehicle pass through the toll gate.
- Anonymized Vehicle number - Anonymized registration number of the vehicle 
- Vehicle type - Type of the vehicle
- Number of axles - Number of axles of the vehicle
- Vehicle code - Category of the vehicle as per the toll plaza.

### tollplaza-data.tsv:
tollplaza-data.tsv is a **tab-separated values** file.

It has the 7 below fields:

- Rowid  - This uniquely identifies each row. This is consistent across all the three files.
- Timestamp - What time did the vehicle pass through the toll gate.
- Anonymized Vehicle number - Anonymized registration number of the vehicle 
- Vehicle type - Type of the vehicle
- Number of axles - Number of axles of the vehicle
- Tollplaza id - Id of the toll plaza
- Tollplaza code - Tollplaza accounting code.

### payment-data.txt:
payment-data.txt is a **fixed width** file. Each field occupies a fixed number of characters.

It has the below 7 fields:

- Rowid  - This uniquely identifies each row. This is consistent across all the three files.
- Timestamp - What time did the vehicle pass through the toll gate.
- Anonymized Vehicle number - Anonymized registration number of the vehicle 
- Tollplaza id - Id of the toll plaza
- Tollplaza code - Tollplaza accounting code.
- Type of Payment code - Code to indicate the type of payment. Example : Prepaid, Cash.
- Vehicle Code -  Category of the vehicle as per the toll plaza.




