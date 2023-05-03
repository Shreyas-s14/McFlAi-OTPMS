from json import loads
from kafka import KafkaConsumer
import sys
import psycopg2
import pandas as pd
import sqlite3

con = sqlite3.connect('nille.db')
cur = con.cursor()
# Create table
#cur.execute(
#    '''CREATE TABLE kafkastream
#       (key text, value text)''')

consumer = KafkaConsumer(
        sys.argv[1],
        bootstrap_servers= ['localhost : 9092'],
        #auto_offset_reset = 'earliest',
        #enable_auto_commit = True,
        #group_id = 'my-group',
        #value_deserializer = lambda x : loads(x.decode('utf-8'))
        )
try:
    for message in consumer:
        print(message.key.decode('utf-8'))
        key = message.key.decode('utf-8')
        value = message.value.decode('utf-8')
        #print("INSERT INTO kafkastream VALUES ("+ key + "," + str(value)+")")
        cur.execute("INSERT INTO kafkastream VALUES ("+ key + "," + str(value)+")")
        # Save (commit) the changes

except KeyboardInterrupt:
    print("\n Airport Closed \n ")
#except:
#    print("\n Catastrophe ! \n")
finally:
    con.commit()
    con.close()
   


