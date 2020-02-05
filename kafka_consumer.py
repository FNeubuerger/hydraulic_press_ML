import json
import datetime
import csv
import logging
import os
import datetime
import glob
import math

import random
import warnings

import subprocess

import numpy as np
import pandas as pd

from time import time, sleep
from json import dumps

from kafka import KafkaProducer

from sklearn import datasets
from sklearn.preprocessing import MinMaxScaler

from sklearn.metrics import accuracy_score, f1_score, log_loss   # The lower the better

from sklearn.model_selection import train_test_split

from sklearn.linear_model import SGDClassifier

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.style as style

import seaborn as sns

matplotlib.rcParams['font.family'] = 'fantasy'
matplotlib.rcParams['font.weight'] = 3
matplotlib.rcParams['font.size'] = 10

style.use('bmh')

from kafka import KafkaConsumer, TopicPartition
from json import loads



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


def chart_save_image(plt=None, f_size=None, left=None, right=None, bottom=None, top=None, wspace=None, hspace=None, fileName=None):
    ''' Save the chart image with the set of specific options '''

    fig = plt.gcf()
    fig.set_size_inches(8, 4.5) # To maintain the 16:9 aspect ratio

    if f_size :
        fig.set_size_inches(f_size[0], f_size[1])

    # https://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.subplots_adjust

    # left          = 0.125     # the left side of the subplots of the figure
    # right         = 0.9       # the right side of the subplots of the figure
    # bottom        = 0.125     # the bottom of the subplots of the figure
    # top           = 0.9       # the top of the subplots of the figure
    # wspace        = 0.0       # the amount of width reserved for blank space between subplots,
    #                           # expressed as a fraction of the average axis width
    # hspace        = 0.0       # the amount of height reserved for white space between subplots,
    #                           # expressed as a fraction of the average axis height

    plt.subplots_adjust(left=left, bottom=bottom, right=right, tp=top, wspace=wspace, hspace=hspace)
    plt.savefig(f'{fileName}')
    plt.clf()


def check_kafka_prcocess(logger=None):
    ''' Check if the kafka process is running or not '''
    # All Kafka brokers must be assigned a broker.id. On startup a broker will create an ephemeral node in Zookeeper with a path of /broker/ids/$id 

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


def get_topic_offset(logger=None, topic_name=None):
    ''' To return the topic offset to break the continous consumer listening to the message broker '''

    consumer = KafkaConsumer(  bootstrap_servers=['localhost:9092'],
                               auto_offset_reset='earliest',
                               enable_auto_commit=True,
                               group_id='consumer_1_group_1',   # The group_id is used by kafka to store the latest offset
                               value_deserializer=lambda x: loads(x.decode('utf-8'))
                               )

    tp = TopicPartition(topic_name,0)
    consumer.assign([tp])

    consumer.seek_to_end(tp)
    lastOffset = consumer.position(tp)
    consumer.close()

    return lastOffset


def initial_model(logger=None):
    ''' Simulation of the iniital model setup in a traditional ML training '''

    clf = SGDClassifier(
        # loss='hinge', # hinge as a loss gives the linear SVM
        loss='log', # log as a loss gives the logistic regression
        # loss='perceptron', # perceptron as a loss gives perceptron
        # penalty='elasticnet', # l2 as a default for the linear SVM
        penalty='none', # l2 as a default for the linear SVM
        # penalty='l2', # l2 as a default for the linear SVM
        fit_intercept=True, # defaults to True
        shuffle=True, # shuffle after each epoch might not have multiple epoch as the parital fit does not have max_iter
        # alpha=0.00008,ht dran geda
        # eta0=0.00001,
        eta0=0.001,
        # learning_rate='optimal',
        learning_rate='constant',
        average=False, # computes the averaged SGD weights and stores the result in the coef_
        random_state=2011,
        verbose=0,
        max_iter=1000,
        warm_start=False
        # better to set the warm_start false since the multiple fit can result in diff models
        # True as an option improves the classification metrics where False does not
    )

    return clf



def consumer_train_model(logger=None, topic_name=None, topic_offset=None):
    ''' Consume the kafka message broker stream and partial fit the model '''

    row_list = []
    ll_list = []
    accuracy_list = []
    f1_list = []

    selected_models = 0

    clf = initial_model(logger=logger)

    consumer = KafkaConsumer(
                               topic_name,
                               bootstrap_servers=['localhost:9092'],
                               auto_offset_reset='earliest',
                               enable_auto_commit=True,
                               # group_id='consumer_1_group_2',   # The group_id is used by kafka to store the latest offset
                               value_deserializer=lambda x: loads(x.decode('utf-8'))
                               )

    counter = 1

    for message in consumer:
        message = message.value
        
        X = np.array(message['X'])
        y = np.array(message['y'])
        print(X)

        clf.partial_fit(X,y) # partial Fit the model to be implemented. or update a frozen TF Graph


    consumer.close()

    return None



def main(logger=None, kafka_path=None):
    ''' Main routine to call the entire process flow '''

    # Main call --- Process starts

    logger.info(f'')
    logger.info(f'{"-"*20} List all kafka topics - starts here {"-"*20}')
    logger.info(f'')

    base_clf = initial_model(logger=logger)

    kafka_status = check_kafka_prcocess(logger=logger)

    if kafka_status :
        logger.info('Kafka stream is active')
        topic_offset = get_topic_offset(logger=logger, topic_name='testbroker')
        logger.info(f'Topic offset : {topic_offset}')
        consumer_train_model(logger=logger, topic_name='testbroker', topic_offset=topic_offset)

    logger.info(f'')
    logger.info(f'{"-"*20} List all kafka topics - ends here {"-"*20}')
    logger.info(f'')

    # Main call --- Process ends

    return None



if __name__ == "__main__":

    warnings.filterwarnings("ignore", category=DeprecationWarning)


    LOG_TS = datetime.datetime.now().strftime('%Y.%m.%d.%H.%M.%S')
    LOG_LEVEL = logging.DEBUG
    # (LogLevel : Numeric_value) : (CRITICAL : 50) (ERROR : 40) (WARNING : 30) (INFO : 20) (DEBUG : 10) (NOTSET : 0)

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

    kafka_path = '~/kafka_2.11-2.1.0/'

    Test_case = f'Kafka consumer code module : {LOG_TS}'
    Test_comment = '-' * len(Test_case)

    logger.info(Test_comment)
    logger.info(Test_case)
    logger.info(Test_comment)

    delete_old_log_files(delete_flag=DELETE_FLAG, logger=logger, extension_list=extension_list)
    main(logger=logger, kafka_path=kafka_path)


    logger.info(Test_comment)
    logger.info(f'Code execution took {round((time() - ts), 4)} seconds')
    logger.info(Test_comment)