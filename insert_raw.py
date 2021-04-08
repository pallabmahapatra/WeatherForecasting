#!/usr/bin/env python
import sys
import os
import pandas as pd
import pymongo
import json
import time
from datetime import datetime,timezone
import urllib.request, json

print("Starting main thread...")

print("Loading Config file...")
file = 'configuration.json'

with open(file) as f:
    configuration = json.load(f)

def import_content(filepath,temp = False):

    setname = configuration['mongodb']['repset']
    mongoservers = configuration['mongodb']['servers']
    user = configuration['mongodb']['user']
    password = configuration['mongodb']['password']
    authSource = configuration['mongodb']['database']
    mng_client = pymongo.MongoClient(mongoservers, replicaSet=setname, username=user, password=password, authSource=authSource)

    mng_db = mng_client[authSource] 
    #collection_name = 'Original_Weather'  #CHANGE FOR EACH COLLECTION
    db_cm = mng_db[collection_name]
    
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)
    data = pd.read_csv(file_res,na_filter=False)
    #print(data.iloc[:4])
    
    # = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data["MeasurementDT"]))
    #data["MeasurementDT"] = pd.to_datetime(data['MeasurementDT']).dt.strftime('%Y-%m-%d %H:%M:%S')
    print(data)
    
    data_json = json.loads(data.T.to_json()).values()
    
    
    if (temp != False):
        
        url = "http://api.openweathermap.org/data/2.5/weather?q=Honolulu,USA&APPID=30a3b7dbdda94978cffdf11acd381b03&units=imperial"
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode())
        print(type(data))
        now = datetime.now(timezone.utc)
        #print("now =", now)
        dt_string = now.strftime('%Y-%m-%d %H:%M:%S')
        data_dt = {'dt':dt_string}
        data_temp = data['main']
        data_json = {**data_dt, **data_temp}

        
    db_cm.insert(data_json)

if __name__ == "__main__":

     print ("Running....", "\n")
     filepath = "C:\\Users\\Pallab\\Desktop\\Weather_Cloud_All.csv"
     import_content(filepath)

     
        
