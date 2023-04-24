from json import loads
from kafka import KafkaConsumer


consumer = KafkaConsumer(
        'airport',
        bootstrap_servers= ['localhost : 9092'],
        value_deserializer = lambda x : x.decode('utf-8')
        )
try:
    for message in consumer:
        print(str(message.value))
except KeyboardInterrupt:
    print("\n Airport Closed \n ")
except:
    print("\n Catastrophe ! \n")
