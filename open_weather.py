import urllib.request, json
import time
import pandas as pd

def inbound_connector():

    url = "http://api.openweathermap.org/data/2.5/weather?q=Minneapolis,USA&APPID=30a3b7dbdda94978cffdf11acd381b03&units=imperial"
    response = urllib.request.urlopen(url)

    data = json.loads(response.read().decode())
    data_dt = pd.Series({'dt':time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data["dt"]))})
    data_final = pd.Series(data['main'])
    

    result = pd.concat([data_dt, data_final], axis=0, sort=False)
    print (result)


inbound_connector()
