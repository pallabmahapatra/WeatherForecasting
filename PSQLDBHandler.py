
# PSQLDBHandler.py
# - Pulls and pushes data into the Postgres table.

# Imports #
from sqlalchemy import create_engine
import json
import pandas as pd
import numpy as np

with open('../config.json') as config_file:
    config = json.load(config_file)


postgres_url = config["POSTGRES_URL"]
postgres_real_time_table_name = config["operational_weather_table_name"]
postgres_historical_table_name = config["historical_weather_table_name"]
observation_stations = config["UYNIVERSITY_HAWAII_LOCATIONS"]
obs_stations_table = config["postgres_locations_table_name"]
raw_data_table = config["postgres_raw_data_table_name"]


def construct_stations_table():
    if check_table_exists(obs_stations_table):
        print("Observation stations already stored in Postgres...")
        return

    df = pd.DataFrame([])
    station_ids = []
    latitude = []
    longitude = []
    names = []
    for key in observation_stations:
        station_ids = station_ids + [key]
        latitude = latitude + [observation_stations[key][1]]
        longitude = longitude + [observation_stations[key][0]]
        names = names + [observation_stations[key][2]]
    df["station_id"] = station_ids
    df["latitude"] = latitude
    df["longitude"] = longitude
    df["names"] = names

    engine = create_engine(postgres_url)
    df.to_sql(obs_stations_table, engine)
    engine.execute(f'ALTER TABLE {obs_stations_table} ADD PRIMARY KEY (station_id)')

    print("Building table to hold observation stations metadata....")


def check_table_exists(table_name):
    """Checks if the given database name exists or not.

    :param table_name:
    :return:
    """
    engine = create_engine(postgres_url)
    return engine.dialect.has_table(engine, table_name)


def get_values_for_measurement_from_postgres(measurement, station):
    """Gets values for a particular weather measurement from a particular station.

    :param measurement: Weather measurement string
    :param station: observation station ID
    :return: JSON with all the values
    """
    engine = create_engine(postgres_url)
    if not check_table_exists(postgres_real_time_table_name):
        print("Table does not exist")
        return []
    result_set = engine.execute(
        f'SELECT "times","{measurement}" FROM {postgres_real_time_table_name} WHERE {postgres_real_time_table_name}'
        f'.station = \'{station}\'')
    return_set = [result_set_ for result_set_ in result_set]

    return return_set


def store_raw_data_in_postgres(raw_dataframe):
    """Stores raw data as obtained from NWS into table for raw data.

    :param raw_dataframe:
    :return:
    """
    engine = create_engine(postgres_url)
    raw_dataframe.to_sql(raw_data_table, engine, if_exists='append')

    print("Storing raw data in Postgres.....")


def store_real_time_data_postgres(dataframe):
    """Stores JSON object into InfluxDB with provided host name and port number.

    :param dataframe:
    :return:
    """
    engine = create_engine(postgres_url)
    dataframe.to_sql(postgres_real_time_table_name, engine, if_exists='replace')

    construct_stations_table()
    engine.execute(
        f'ALTER TABLE {postgres_real_time_table_name} ADD FOREIGN KEY (station) REFERENCES '
        f'{obs_stations_table}(station_id)')

    print("Storing operational data in Postgres.....")


def store_historical_data_postgres(dataframe):
    """Stores JSON object into InfluxDB with provided host name and port number.

    :param dataframe:
    :return:
    """
    engine = create_engine(postgres_url)
    dataframe.to_sql(postgres_historical_table_name, engine, if_exists='append')

    construct_stations_table()
    engine.execute(
        f'ALTER TABLE {postgres_historical_table_name} ADD FOREIGN KEY (station) REFERENCES '
        f'{obs_stations_table}(station_id)')

    print("Storing historical data in Postgres.....")


def store_data_postgres(dataframe):
    """Stores data in postgres that has been formatted for Postgres.

    :param dataframe:
    :return:
    """
    store_historical_data_postgres(dataframe)
    store_real_time_data_postgres(dataframe)


def drop_table(table_name):
    """Drops table from database.

    :return:
    """
    engine = create_engine(postgres_url)
    if check_table_exists(table_name):
        print("It exists")
        engine.execute(f'DROP TABLE {table_name}')
        return
    print("It does not exist")

drop_table(obs_stations_table)
