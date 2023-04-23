from json import loads
from kafka import KafkaConsumer


consumer = KafkaConsumer(
        'airport',
        bootstrap_servers= ['localhost : 9092'],
        #auto_offset_reset = 'earliest',
        #enable_auto_commit = True,
        #group_id = 'my-group',
        value_deserializer = lambda x : loads(x.decode('utf-8'))
        )
try:
    for message in consumer:
        print(str(message.value) + " received")
except KeyboardInterrupt:
    print("\n Airport Closed \n ")
except:
    print("\n Catastrophe ! \n")


