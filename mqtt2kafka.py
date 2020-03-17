import paho.mqtt.client as mqttClient
import time
from time import sleep
from json import dumps
from kafka import KafkaProducer

def on_connect(client, userdata, flags, rc):
 
    if rc == 0:
 
        print("Connected to broker")
 
        global Connected                #Use global variable
        Connected = True                #Signal connection 
 
    else: 
        print("Connection failed")
 
def on_message(client, userdata, message):
    producer.send('data', value=message)
 
Connected = False   #global variable for the state of the connection
#set up mqtt
broker_address= ""  #Broker address
port = 123456                         #Broker port
user = "yourUser"                    #Connection username
password = "yourPassword"            #Connection password
 
client = mqttClient.Client("Name")               #create new instance
client.username_pw_set(user, password=password)    #set username and password

#set up Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                              value_serializer=lambda x: 
                              dumps(x).encode('utf-8'))
#do the sending
client.on_connect = on_connect                      #attach function to callback
client.on_message = on_message                     #attach function to callback
 
client.connect(broker_address, port=port)          #connect to mqtt broker
 
client.loop_start()        #start the loop
 
while Connected != True:    #Wait for connection
    time.sleep(0.1)
 
client.subscribe("topic")
 
try:
    while True:
        time.sleep(1)
 
except KeyboardInterrupt:
    print("exiting")
    client.disconnect()
    client.loop_stop()