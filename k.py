from kafka import KafkaProducer
import csv
from time import sleep

# Create a Kafka producer object
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Define the CSV data to send
csv_data = [
  ["Aircraft1", 10.0],
  ["Aircraft2", 5.0],
  ["Aircraft1", 15.0],
  ["Aircraft3", 8.0],
  ["Aircraft2", 12.0],
]

# Convert the CSV data to a string and send it to Kafka
for row in csv_data:
  csv_string = ','.join(map(str, row))
  sleep(1)
  producer.send('aircraft', value=csv_string.encode('utf-8'))
