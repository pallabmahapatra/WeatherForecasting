

# Executable_Generalized_utility.py
# - File for Valueing the data from 4 colleges(Honolulu,Kapiolani,Leeward,Windward) in \n
#   mongoDB(Value_ProcessedData_Historical) in a configurable format of ('Time','Value','Portfolio') 



#!/usr/bin/env python
import sys
import os
import pandas as pd
import pymongo
import json
import time
import kafka
from threading import Thread
from datetime import datetime,timezone, date 
import urllib.request, json
from sys import argv 
from time import sleep
from kafka import KafkaConsumer
import Common.Logger as log
import Common.BaseConnector as bc

pd.set_option('display.max_columns',None) 

print("Starting main thread...") 

print("Loading Config file...")

file = "configuration-int.json"

with open(file) as f:
    config = json.load(f) 
    
def import_content(collection_name):

    setname = config['mongodb']['repset']
    mongoservers = config['mongodb']['servers']
    user = config['mongodb']['user']
    password = config['mongodb']['password']
    authSource = config['mongodb']['database']
    mng_client = pymongo.MongoClient(mongoservers, replicaSet=setname, username=user, password=password, authSource=authSource)

    mng_db = mng_client[authSource] 
    collection__name = collection_name 
    db_cm = mng_db[collection__name]
    return db_cm 


def forecastnow(message):
    
    submittedBy = message['header']['submittedBy']
# #     #log.logInfo(submittedBy)
    submittedDate = message['header']['submittedDate']
#     #log.logInfo(submittedDate)
    db_cm = import_content('Load_RawData_RT')
    raw_data = db_cm.find()
    data = pd.DataFrame(list(raw_data))
    data = data.iloc[:,1:]   
    data = data.set_index(['MeasurementDT'])
    data.index = pd.to_datetime(data.index)
    # data = data[data.Portfolio == 2]
    df_resample = data.resample('15T').mean()
    df_interpolate = df_resample.interpolate(method = 'linear',limit_direction = 'forward',axis = 0)
    processed_list = list()
    for index,row in df_interpolate.iterrows():
        print('Inside for loop')
        log.logInfo(index)
        log.logInfo(row)
        _object_ =  {'MeasurementDT' : None , 'Load' : None , 'Portfolio' : int(row.get('Portfolio')) ,
                     'Update_Date' : submittedBy, 'UpdateBy' : submittedDate}

        if pd.isnull(index):
            _object_['MeasurementDT'] = None
        else:
            _object_['MeasurementDT'] = index
      
      
        if pd.isnull(row.get('Load')):
            _object_['Load'] = None
        else:
            _object_['Load'] = int(row.get('Load'))
                    
        processed_list.append(_object_)

    db_cm = import_content('Load_ProcessedData_RT')
# #     for i in processed_list:
# #         db_cm.insert_one(i)
    db_cm.insert_many(processed_list)
#     # db_cm.updateMany{ _id: { $exists: true }update( , 
#     # db.collection.updateMany(query, update, {upsert: true})
    
    return True


def ConsumerGroup():
    # Consumes messages from Kafka topic [argus-generation, argus-interchange, argus-SIT, argus-WAAV]
    # Send messages to MongoDB
    try:
        print('Inside Consumer group')
        servers = config["kafka"]["servers"]
        topics = "websmartforecast-heco-model-service"
        group = config["bulkload"]['group']
        consumergroup = KafkaConsumer(bootstrap_servers=servers, group_id=group)
        consumergroup.subscribe(topics)
        print('Initialize kafka done')
        #forecastnow(message = None)
        # Continuously check messages for consumergroup topics
        while(True):
            try:
                print('Checking for data')
                raw_messages = consumergroup.poll(timeout_ms=1000, max_records=5000)
                # log.logInfo(raw_messages)
                for topic_partition, message in raw_messages.items():
                    print(topic_partition)
                    print(message)
                    print("Checking messages in the consumergroup topics... ")
                    # Decode/load message and get respective topic
                    mess = message[0].value.decode('ascii')
                    topic = message[0].topic
                    obj = json.loads(mess)
                    forecastnow(obj)
                    log.logInfo(obj)

            except Exception as e:
                error = "Bad message structure: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
                log.logError(error)

    except Exception as e:
        error = "Unable to consume messages: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
        log.logError(error)
        sleep(5)



class ConsumerGroupThread(Thread):
    # Thread for ConsumerGroup
    def run(self):
        log.logInfo('Inside run function')
        log.logInfo("Starting Consumer Group Thread")
        ConsumerGroup()
        log.logInfo("Completed Consumer Group Thread")

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
        consumergroupThread.run()
    except Exception as e:
        error = "Unable to run ConsumerGroupThread: {}|{}".format(e, sys.exc_info()[2].tb_lineno)
        log.logError(error)

if __name__ == '__main__':
    main()
log.logInfo("Running Script Successfully....")  