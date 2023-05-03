'''
Schema of db:
refer spark_stream.py

'''
import mysql.connector
from kafka import KafkaConsumer
from json import loads
from sys import argv
from create_db import create_table

Consumer = None
Topic = argv[1]

colum = ["Year","Month","DayofMonth","DayOfWeek","DepTime","CRSDepTime","ArrTime","CRSArrTime","UniqueCarrier","FlightNum","TailNum","ActualElapsedTime","CRSElapsedTime","AirTime","ArrDelay","DepDelay","Origin","Dest","Distance","TaxiIn","TaxiOut","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"]

def init_db():
	global DB_CON
	DB_CON = mysql.connector.connect(
		host="localhost",
		database="mcFlAi",
		user="root",
		password="password"
    )
	

cur = DB_CON.cursor()	
def insert_db(data):
	global DB_CON
    

def init_consumer():
    global consumer
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )    
