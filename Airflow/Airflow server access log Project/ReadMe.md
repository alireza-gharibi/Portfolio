I've created a DAG to Extract-Transform-Load the server access log file which is available at the [URL](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt).

The server access log file contains these fields:

- timestamp - TIMESTAMP

- latitude - float

- longitude - float

- visitorid - char(37)

- accessed_from_mobile - boolean

- browser_code - int

## Tasks: 
- **download** task must download the server access log file
- The **extract** task must extract the fields timestamp and visitorid.
- The **transform** task must make the visitorid lower-case.
- The **load** task must compress the extracted and transformed data
