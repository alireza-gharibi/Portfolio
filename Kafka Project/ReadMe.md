## Scenario:
 I am a data engineer at a data analytics consulting company. I have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. As a vehicle passes a toll plaza, the vehicle’s data like **vehicle_id, vehicle_type, toll_plaza_id** and **timestamp** are streamed to Kafka. My job is to create a data pipeline that collects the streaming data and loads it into a database.
## Preparing the Environment:
Starting postgres database container in docker:
```
docker run --rm -e POSTGRES_PASSWORD=123456 -p 5432:5432 postgres
```
Open a new terminal or use **-d** in previous code. Install **Postgres CLI**:
```
sudo apt-get install postgresql  
```
Connecting to postgres: 
```
psql --host=localhost --port=5432 --user=postgres   
```
You might need to enter password, i set it when starting the container. \
Now we are in **postgres cli** and can enter postgres(sql) commnads.

Creating database:
```
create database tolldata;  
```
Choosing database to use:
```
\c tolldata;   
```
Creating table schema:
```
create table livetolldata(timestamp timestamp,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);  
```
Now we exit **postgres cli** and go back to shell:
```
\q
```
Kafka client module needed for python:
```
pip install kafka
```
We also need to install postgresql python api:
```
pip install psycopg2
```
We need to download and install [kafka](https://kafka.apache.org/quickstart)

Start Zookeeper:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Start Kafka server:
```
bin/kafka-server-start.sh config/server.properties
```
Creating the topic in kafka:
```
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic toll 
```
### Running [Producer](https://github.com/alireza-gharibi/Portfolio/blob/main/Kafka%20Project/toll_traffic_generator.py) Script:
```
python3 toll_traffic_generator.py
```

### Running [Consumer](https://github.com/alireza-gharibi/Portfolio/blob/main/Kafka%20Project/streaming_data_reader.py) Script:

```
python3 streaming_data_reader.py
```
\
**Check postgresql DB for results!**



