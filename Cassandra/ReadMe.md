## Scenario:
I am a data engineer at a data analytics consulting company. My company prides itself in being able to efficiently handle data in any format on any database on any platform. Analysts in my office need to work with data on different databases, and data in different formats. While they are good at analyzing data, they count on me to be able to move data from external sources into various databases, move data from one type of database to another, and be able to run basic queries on various databases.
### Objective: 
- Import data into a Cassandra database.
- Query data in a Cassandra database.
- Export data from Cassandra.

In this project i import the CSV file(**partial_data.csv** which was exported from MongoDB to CSV) to Cassandra.\
So let's continue where i left off. take look at [MongoDB project](https://github.com/alireza-gharibi/Portfolio/tree/main/Mongo%20DB) too.

### Preparing Environment:
Starting cassandra containers: \
I create a home lab on my pc:
```
docker network create cass-network
```
```
docker run --rm  --name cassandra-1 --network cass-network cassandra
```
```
docker run --rm  --name cassandra-2 --network cass-network -e CASSANDRA_SEEDS=cassandra-1 cassandra
```
```
-docker run -it --rm --network cass-network cassandra cqlsh cassandra-1  

```
 ### Task 1: 
 Import **partial_data.csv** into cassandra server into a keyspace named **entertainment** and a table named **movies**: \
 **Cassandra does not allow you to create a column starting with underscore '_'**
```
CREATE KEYSPACE entertainment 
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 2};

```
```
use entertainment; 
```
```
CREATE TABLE movies(
id int PRIMARY KEY,
title text,
year int,
rating text,
 director text
);

```
```
COPY entertainment.movies(id,title,year,rating,director) FROM 'partial_data.csv' WITH DELIMITER=',' AND HEADER=TRUE;

```
### Task 2:
Write a cql query to count the number of rows in the **movies** table.
```
SELECT COUNT (*) FROM movies;

```
### Task 3:
Create an index for the **rating column** in the **movies** table using cql.
```
CREATE INDEX IF NOT EXISTS rating_index ON entertainment.movies (rating);
```
### Task 4:
 Write a cql query to count the number of movies that are rated **G**.
 ```
 SELECT COUNT (*) FROM movies WHERE rating='G';
 ```
 ### Task 5: 
 Export the **movies** table into a csv file:
 ```
 COPY movies TO **cassandra-movies.csv**;
 ```







