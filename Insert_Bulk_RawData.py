
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
from datetime import datetime,timezone, date 
import urllib.request, json
from sys import argv 


print("Starting main thread...") 

print("Loading Config file...")
file = "configuration.json"

with open(file) as f:
    configuration = json.load(f) 
 

path = argv[1]
portfolio = argv[2]
raw_collectionname = argv[3]
processed_collectionname = argv[4]

Update_Date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
Update_By = argv[5]
# col_1,col_2 = argv[4],argv[5]

def import_content(collection_name):

    setname = configuration['mongodb']['repset']
    mongoservers = configuration['mongodb']['servers']
    user = configuration['mongodb']['user']
    password = configuration['mongodb']['password']
    authSource = configuration['mongodb']['database']
    mng_client = pymongo.MongoClient(mongoservers, replicaSet=setname, username=user, password=password, authSource=authSource)

    mng_db = mng_client[authSource] 
    collection__name = collection_name  #CHANGE FOR EACH COLLECTION
    db_cm = mng_db[collection__name]
    return db_cm


#file_open_1 = pd.read_excel(path , sheet_name = 'Data' , usecols = 'col_1','col_2')
file_open_1 = pd.read_excel(path, sheet_name = 'Data' , usecols = ['Timestamp','Value'])
file_open_1 = file_open_1.drop(file_open_1.index[0])
data = file_open_1.set_index(['Timestamp'])
data['Value'] = data['Value'].astype(float)

raw_create_list = list()

for index,row in data.iterrows():
    _object_ =  {'MeasurementDT' : None , 'Load' : None , 'Portfolio' : int(portfolio) ,'Update_Date' : Update_Date, 'UpdateBy' : Update_By}

    if pd.isnull(index):
        _object_['MeasurementDT'] = None
    else:
        _object_['MeasurementDT'] = str(index)
        
    
    if pd.isnull(row.get('Value')):
        _object_['Load'] = None
    else:
        _object_['Load'] = row.get('Value')
                
    raw_create_list.append(_object_)

db_cm = import_content(raw_collectionname)
db_cm.insert_many(raw_create_list)


data = pd.DataFrame(list(db_cm.find()))
data = data.iloc[:,1:3]
data = data.set_index(['MeasurementDT'])
data.index = pd.to_datetime(data.index)

df_resample = data.resample('15T').mean()

df_interpolate = df_resample.interpolate(method = 'linear',limit_direction = 'forward',axis = 0)

processed_create_list = list()

for index,row in df_interpolate.iterrows():
    _object_ =  {'MeasurementDT' : None , 'Load' : None , 'Portfolio' : int(portfolio) ,'Update_Date' : Update_Date, 'UpdateBy' : 'AnshulM'}

    if pd.isnull(index):
        _object_['MeasurementDT'] = None
    else:
        _object_['MeasurementDT'] = str(index)
        
    
    if pd.isnull(row.get('Load')):
        _object_['Load'] = None
    else:
        _object_['Load'] = row.get('Load')
                
    processed_create_list.append(_object_)

processed_db_cm = import_content(processed_collectionname)
processed_db_cm.insert_many(processed_create_list)


print ("Running Script Successfully....", "\n")

  