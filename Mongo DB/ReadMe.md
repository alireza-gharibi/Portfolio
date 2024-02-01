## Scenario:
I am a data engineer at a data analytics consulting company. my company prides itself in being able to efficiently handle data in any format on any database on any platform. Analysts in my office need to work with data on different databases, and data in different formats. While they are good at analyzing data, they count on me to be able to move data from external sources into various databases, move data from one type of database to another, and be able to run basic queries on various databases.
## Objectives
- Import data into a MongoDB database.
- Query data in a MongoDB database.
- Export data from MongoDB.
## Preparing Environment
- First off, i use docker to start a mongodb container:
```
docker run --rm -p 27017:27017 mongo
```
By default Mongo's configuration for this [image](https://hub.docker.com/_/mongo) requires no authentication for access, even for the administrative user.

- Downloaded install **mongosh** from [here](https://www.mongodb.com/try/download/shell)

- Download and install **mongoimport** and **mongoexport** which is included in this [file](https://www.mongodb.com/try/download/database-tools)

By simply entering **mongosh** on terminal i connect to database using mongo shell and start writing queries.

## Tasks
Now everything is ready, let's insert data into database and query it.
### Task 1
Import **movies.json** into mongodb server into a database named **entertainment** and a collection named **movies**:
```
mongoimport  --db entertainment --collection movies --file movie.json --jsonArray
```
[mongoimport](https://www.mongodb.com/docs/database-tools/mongoimport/#mongodb-binary-bin.mongoimport) is a shell command!
### Task 2
Write a mongodb query to find the year in which most number of movies were released:
```
db.movies.aggregate([{$group:{"_id":"$year", "yearly_sum":{$sum:1}}}, {$sort: {"yearly_sum": -1}},{ $limit : 1 }])
```
### Task 3
Write a mongodb query to find the count of movies released after the year 1999:
```
db.movies.aggregate([{$match:{year:{$gt:1999}}}, {$count:"movie_count"}])
```
### Task 4
Write a query to find out the average votes for movies released in 2007:
```
db.movies.aggregate([{$match:{year:2007}},{$group:{"_id":"$year", "average_vote":{$avg:"$Votes"}}}])
```
### Task 5
Export the fields _id, “title”, “year”, “rating” and “Director” from the ‘movies’ collection into a file named partial_data.csv:
```
mongoexport  --db entertainment --collection movies --out partial_data.csv --type=csv --fields _id,title,year,rating,Director
```
[mongoexport](https://www.mongodb.com/docs/database-tools/mongoexport/#mongodb-binary-bin.mongoexport) is shell command!


