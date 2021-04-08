import sys
import os
import pandas as pd
import pymongo
import json
import time
import kafka
from threading import Thread
from threading import Timer
from datetime import datetime,timezone, date 
import urllib.request, json
from sys import argv 
from time import sleep
from kafka import KafkaConsumer
import Common.Logger as log
import Common.BaseConnector as bc
import WeatherDataManagement.Preprocessor as ppc

pd.set_option('display.max_columns',None) 
mongo = None
config = None
KafkaMessages =[]

def ConsumerGroup():
    # Consumes messages from Kafka topic 
    # Send messages to MongoDB
    try:
        servers = config["kafka"]["servers"]
        topics = config["kafka"]["topic"]
        group = config["bulkload"]['group']
        consumergroup = KafkaConsumer(bootstrap_servers=servers, group_id=group)
        consumergroup.subscribe(topics)
        log.logInfo("Consumer Group receiving from topic {} ...".format(topics))
        
        # Continuously check messages for consumergroup topics
        while(True):
            global KafkaMessages
            try:
                raw_messages = consumergroup.poll(timeout_ms=1000, max_records=5000)
                for topic_partition, message in raw_messages.items():
                    mess = message[0].value.decode('ascii')
                    topic = message[0].topic
                    obj = json.loads(mess)
                    log.logInfo("Received message from kafka: {}".format(obj))
                    KafkaMessages.append(obj)
            except Exception as e:
                error = "Bad message structure: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
                log.logError(error)
            time.sleep(5)
    except Exception as e:
        error = "Unable to consume messages: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
        log.logError(error)

def Periodic():
    try:
        # Continuously check process messages if available
        while(True):
            global KafkaMessages
            try:
                for obj in KafkaMessages:
                    log.logInfo("Processing message: {}".format(obj))
                    start = obj['payload']['MeasurementStartDT']
                    end = obj['payload']['MeasurementEndDT']
                    if obj['header']['verb'] == "Load":
                        collection = "Load_RawData_RT"
                    elif: obj['header']['verb'] == "Weather Forecast":
                        collection = "Weather_Forecast_RT"
                    
                    elif obj['header']['verb'] == "Weather Actual":
                        collection = "Weather_Actual_RT"
                    else:
                        Log.logError(“Verb not recognized: {}”.format(verb))
                    
                    ppc.preprocess(start, end, mongo, collection)
                    log.logInfo("Finished Preprocessing")
                    KafkaMessages.remove(obj)
            except Exception as e:
                error = "Bad message structure: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
                log.logError(error)
            time.sleep(5)
    except Exception as e:
        error = "Unable to run Periodic Processing function: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
        log.logError(error)

class ConsumerGroupThread(Thread):
    # Thread for ConsumerGroup
    def run(self):
        log.logInfo("Starting Consumer Group Thread")
        ConsumerGroup()
        log.logInfo("Completed Consumer Group Thread")

class PeriodicThread(Thread):
    # Thread for Periodic Processing
    def run(self):
        log.logInfo("Starting Periodic Thread")
        Periodic()
        log.logInfo("Completed Periodic Thread")

def main():

    global config
    global mongo
    global timer

    obj = bc.InitializeHECO("bulkload")
    config = obj["config"]
    mongo = obj["mongo"]
    timer = config["bulkload"]["timer"]

    # Run the ConsumerGroupThread
    try:
        consumergroupThread = ConsumerGroupThread()
        consumergroupThread.start()
    except Exception as e:
        error = "Unable to run ConsumerGroupThread: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
        log.logError(error)

    # Run the PeriodicThread
    try:
        periodicThread = PeriodicThread()
        periodicThread.start()
    except Exception as e:
        error = "Unable to run PeriodicThread: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
        log.logError(error)
        
if __name__ == '__main__':
    main()