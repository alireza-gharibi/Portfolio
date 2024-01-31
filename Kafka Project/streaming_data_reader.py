"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer  # 'pip install kafka' if needed
import psycopg2

TOPIC='toll'
DATABASE = 'tolldata'
USERNAME = 'postgres'
PASSWORD = '123456'

print("Connecting to the database")
try:
    connection = psycopg2.connect(host='localhost', database=DATABASE, user=USERNAME, password=PASSWORD, port = 5432)   # creating connection object
except Exception:
    print("Could not connect to database. Please check credentials")
else:
    print("Connected to database")
cursor = connection.cursor()          # creating cursor object

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)  # creating conumer object which contains all the messages 
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from kafka: kafka messages contain (topic,message). extracting the messages with .value method:

    message = msg.value.decode("utf-8")       #   de-serializing messages

    
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",") #  message are sent as strings from producer and we use '.split' to convert it in to a list.
    # each list element is assigned to each tuple element. timestamp, vehcile_id,... are our column values for the table in our database

    
      # Transform the date format to suit the database schema:
    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')  # "timestamp" is a 'string' and we should cast it to a 'datetime' object 
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")  # re-assigning the 'timestamp' with new value(previous line "timestamp" casted to a datetime object)

    # Loading data into the database table

    sql = "insert into livetolldata values(%s,%s,%s,%s)"     # SQL command 
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()