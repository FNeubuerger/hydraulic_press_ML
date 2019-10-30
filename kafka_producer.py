#       Zookeeper start
#       cd into unzipped kafka folder
#       bin/zookeeper-server-start.sh config/zookeeper.properties
#       Start the kafka process
#       bin/kafka-server-start.sh config/server.properties

import json
import datetime
import pandas as pd
import logging
import os
import datetime
import glob
import math
import random

import subprocess

import numpy as np

from time import time, sleep
from json import dumps

from kafka import KafkaProducer

def check_kafka_prcocess(logger=None):
    ''' Check if the kafka process is running or not '''
    # All Kafka brokers must be assigned a broker.id. 
    # On startup a broker will create an ephemeral node in Zookeeper with a path of /broker/ids/$id 

    cmd_string = f'echo dump | nc localhost 2181 | grep brokers'
    cmd_output = ''

    try :
        cmd_status = subprocess.check_output(cmd_string, stderr=subprocess.STDOUT, shell=True)
        cmd_output = cmd_status.decode('utf-8').split('\n')[0]
    except Exception as e:
        logger.info(e)

    logger.info(f'Kafka process status : ')

    if len(cmd_output) > 0 :
        logger.info(f'')
        logger.info(f'Running')
        logger.info(f'')
        return 1
    else:
        logger.info(f'')
        logger.info(f'Not Running')
        logger.info(f'')
        return 0

    return None
    

def list_topics(logger=None, kafka_path=None):
    ''' Run the kafka shell command and return the list of available topics '''

    # logger.info(f'kafka_path : {kafka_path}')

    cmd_string = f'{kafka_path}bin/kafka-topics.sh --list --zookeeper localhost:2181'
    list_of_topics = subprocess.check_output(cmd_string, stderr=subprocess.STDOUT, shell=True)
    list_of_topics = [i.lower() for i in list_of_topics.decode("utf-8").split("\n") if len(i) > 0 and i.lower() != '__consumer_offsets']

    logger.info(f'')
    logger.info('List of topics : ')
    logger.info(f'')

    for topic in list_of_topics:
        logger.info(f'{topic}')

    logger.info(f'')

    return list_of_topics


def delete_all_topics(logger=None, kafka_path=None, list_of_topics=None):
    ''' Run the kafka shell command to delete all listed topics '''

    logger.info(f'')
    logger.info('Delete all topics : ')
    logger.info(f'')

    for topic in list_of_topics:

        cmd_string = f'{kafka_path}bin/kafka-topics.sh --zookeeper localhost:2181 -delete -topic {topic}'
        cmd_status = subprocess.check_output(cmd_string, stderr=subprocess.STDOUT, shell=True)
        cmd_output = cmd_status.decode('utf-8').split('\n')[0]

        logger.info(f'{cmd_output}')

    return None


def create_topic(logger=None, kafka_path=None, topic=None):
    ''' Routine will create a new topic assuming delete_all_topics will run before this routine '''

    cmd_string = f'{kafka_path}bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic {topic.lower()}'
    cmd_status = subprocess.check_output(cmd_string, stderr=subprocess.STDOUT, shell=True)
    cmd_output = cmd_status.decode('utf-8').split('\n')[0]

    logger.info(f'')
    logger.info(f'{cmd_output}')
    logger.info(f'')

    return None

def delete_old_log_files(delete_flag=False, logger=None, extension_list=None):
    ''' Function to delete the old log files cleanup process '''

    directory = './'
    file_list = os.listdir(directory)

    if delete_flag :
        logger.info('DELETE_FLAG is set to true')
        logger.info('All previous logfiles will be deleted')

        logger.info(f'')
        logger.info(f'{"-"*20} File deletion starts here {"-"*20}')
        logger.info(f'')

        fileName = ".".join(__file__.split(".")[:-1])

        for item in file_list:
            ext_flag = [ item.endswith(i) for i in extension_list ]
            # logger.info(f'{ext_flag} | {item} | {np.sum(ext_flag)} | {fileName in item}')
            if np.sum(ext_flag) and (fileName in item) and (LOG_TS not in item):
                    os.remove(os.path.join(directory, item))
                    logger.info(f'Deleted file : {item}')

        logger.info(f'')
        logger.info(f'{"-"*20} File deletion ends here {"-"*20}')
        logger.info(f'')

    return None

def run_producer(logger=None, topic=None, df=None, target=None):
    ''' Run a producer to put messages into a topic '''

    # The function simulates the stream of data from a pandas dataframe
    # X         : Features of the dataset/Sensor Data
    # y         : Targets of the prediction

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    logger.info(f'Publishing messages to the topic : {topic}')

    for i in range(len(df)):
        X = df.iloc(i)
        y = df[target][i]
        data = {'X' : X.tolist(), 'y' : y.tolist()}
        # Need to convert the ndarray as it is not serializable and it need to be converted tolist()
        producer.send(f'{topic}', value=data)
        #samplerate of 1 second
        sleep(1)
    return None

def main(logger=None, kafka_path=None, filepath=None):
    ''' Main routine to call the entire process flow '''

    # Main call --- Process starts

    logger.info(f'')
    logger.info(f'{"-"*20} List all kafka topics - starts here {"-"*20}')
    logger.info(f'')

    kafka_status = check_kafka_prcocess(logger=logger)

    if kafka_status :
        list_of_topics = list_topics(logger=logger, kafka_path=kafka_path)

        # *************************************** WARNING ***************************************
        # Do not run the delete_all_topics in a production environment it will delete all topics
        # *************************************** WARNING ***************************************
        delete_all_topics(logger=logger, kafka_path=kafka_path, list_of_topics=list_of_topics)
        # *************************************** WARNING ***************************************
        # Do not run the delete_all_topics in a production environment it will delete all topics
        # *************************************** WARNING ***************************************

        create_topic(logger=logger, kafka_path=kafka_path, topic='testbroker')
        df = pd.read_csv(filepath)
        run_producer(logger=logger, topic='testbroker', df=df, target='Valve Condition')


    logger.info(f'')
    logger.info(f'{"-"*20} List all kafka topics - ends here {"-"*20}')
    logger.info(f'')

    # Main call --- Process ends

    return None


if __name__=="__main__":

    LOG_TS = datetime.datetime.now().strftime('%Y.%m.%d.%H.%M.%S')
    LOG_LEVEL = logging.DEBUG
    
    DELETE_FLAG = True
    extension_list = ['.log','.pkl']  # File extensions to delete after the run
    ts = time()

    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%d/%m %H:%M:%S')
    fh = logging.FileHandler(filename=f'{".".join(__file__.split(".")[:-1])}_{LOG_TS}.log')
    fh.setLevel(LOG_LEVEL)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    random.seed(2011)

    kafka_path = '~/kafka_2.12-2.3.0'

    Test_case = f'Kafka producer code module : {LOG_TS}'
    Test_comment = '-' * len(Test_case)

    logger.info(Test_comment)
    logger.info(Test_case)
    logger.info(Test_comment)

    delete_old_log_files(delete_flag=DELETE_FLAG, logger=logger, extension_list=extension_list)
    main(logger=logger, kafka_path=kafka_path, filepath='../data/processed/downsampled/continuous_data.csv')

    logger.info(Test_comment)
    logger.info(f'Code execution took {round((time() - ts), 4)} seconds')
    logger.info(Test_comment)