from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

if __name__ == "__main__":

    # initialize consumer object
    consumer = KafkaConsumer('raw_data',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='my-group',
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
    # start mongo client
    client = MongoClient('localhost:27017')
    # connect to the collection
    collection = client.data.raw
    # loop over all consumer messages to save them into the DB
    for message in consumer:
        message = message.value
        collection.insert_one(message)
        print('{} added to {}'.format(message, collection))