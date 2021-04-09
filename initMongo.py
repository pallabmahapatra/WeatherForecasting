import sys
import json
import pymongo
import psycopg2

configuration_1 = "configuration-dev.json"
configuration_2 = "configuration-int.json"

heco_collections = ['DayTypes_Info', 
                                    'General_Information', 
                                    'Load_Model_Features_Parameters',
                                    'Load_Model_Metadata',
                                    'Profile_Metadata',
                                    'Profiles',
                                    'Solar_Model',
                                    'Load_ProcessedData_Historical',
                                    'Original_Weather',
                                    'Pickles']

def main():

    print("Starting main thread...")

    print("Loading Config file 1: {}...".format(configuration_1))
    file = configuration_1
    with open(file) as f:
        c1 = json.load(f)
    print("LOADED CONFIG FILE 1: {}".format(configuration_1))

    print("Loading Config file 2: {}...".format(configuration_2))
    file = configuration_2
    with open(file) as f:
        c2 = json.load(f)
    print("LOADED CONFIG FILE 2: {}".format(configuration_2))

    print("Connecting to Mongo for configuration 1...")
    setname_1 = c1['mongodb']['repset']
    mongoservers_1 = c1['mongodb']['servers']
    user_1 = c1['mongodb']['user']
    password_1 = c1['mongodb']['password']
    authSource_1 = c1['mongodb']['database']
    connection_1 = pymongo.MongoClient(mongoservers_1, replicaSet=setname_1, username=user_1, password=password_1, authSource=authSource_1)
    db_1 = connection_1[authSource_1]
    print("CONNECTED TO MONGO FOR CONFIGURATION 1 DATABASE {} ON SERVERS {}".format(authSource_1, mongoservers_1))

    print("Connecting to Mongo for configuration 2...")
    setname_2 = c2['mongodb']['repset']
    mongoservers_2 = c2['mongodb']['servers']
    user_2 = c2['mongodb']['user']
    password_2 = c2['mongodb']['password']
    authSource_2 = c2['mongodb']['database']
    connection_2 = pymongo.MongoClient(mongoservers_2, replicaSet=setname_2, username=user_2, password=password_2, authSource=authSource_2)
    db_2 = connection_2[authSource_2]
    print("CONNECTED TO MONGO FOR CONFIGURATION 2 DATABASE {} ON SERVERS {}".format(authSource_2, mongoservers_2))

    print("Copying from {} to {}".format(c1['version']['environment'], c2['version']['environment']))
    for collection in heco_collections:
        col1 = db_1[collection]
        col2 = db_2[collection]
        col2.delete_many({})
        query = {}
        cursor = col1.find(query)
        for item in cursor:
            col2.insert_one(item)
    print("Finished copying from configuration 1 to configuration 2")



if __name__ == '__main__':
    main()
