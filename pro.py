from time import sleep
from json import dumps
from kafka import KafkaProducer
import csv
import sys

data_sender = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        value_serializer = lambda x : dumps(x).encode('utf-8')
        )

l = len(sys.argv)

if (l !=2):
    print(" \nUsage is 'python3 airport.py <csv filename>' \n") 
    sys.exit()

try:
    with open(sys.argv[1]) as source:
        header = next(source)
        rows = csv.reader(source)
        num = 0
        for row in rows:
            #print(row)
            print("Row"+str(num)+ "sent")
            data_sender.send('airport',value = row)
            num = num+1
            sleep(1)


except FileNotFoundError:
    print("\n You've put a wrong file you nitwit\n")
except KeyboardInterrupt:
    print("\n You have grounded this flight \n")
#except :
#    print("\n Just hope this program is the only thing that has crashed !\n")
