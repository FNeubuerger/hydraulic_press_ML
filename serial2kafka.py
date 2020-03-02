from time import sleep
from json import dumps
from kafka import KafkaProducer
import serial
import datetime
from pymongo import MongoClient

if __name__ == "__main__":

    # Configuration
    serial_port = '/dev/...'
    #additional metadata 

    # Connect to Serial Port for communication
    ser = serial.Serial(serial_port, 9600, timeout=0)

    # initialize the Kafka Producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                              value_serializer=lambda x: 
                              dumps(x).encode('utf-8'))
    while True:
        # read data from the serial interface
        values = ser.readline()
        # dictionary
        data = {'values' : values}
        # publishes data to the kafka topic "raw_data"
        producer.send('raw_data', value=data)

      
    