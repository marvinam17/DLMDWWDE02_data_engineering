import os
import json
from kafka import KafkaProducer
from time import sleep
from pandas import DataFrame
from dwd_crawler import (get_and_unzip_files, 
                         get_station_dwd_file_storage)

KAFKA_LISTENER = os.getenv("KAFKA_LISTENER")
STREAM_SPEED = float(os.getenv("STREAMING_SPEED"))
STATION_ID = os.getenv("STATION_ID")
TOPIC_TEMP = "temperature"
TOPIC_OTHE = "others"

def calc_speed_factor(speed:float):
    """
    This is a minor reformat of the streamspeed to give the user a ground speed
    of one day per second.
    """
    streamspeed = 1/(8640*speed)
    return streamspeed

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_LISTENER], 
    value_serializer=serializer
)

def push_records(data:DataFrame, speed:float):
    for _, row in data.iterrows():
        base_payload = {"measurement_date":int(row["MEASUREMENT_DATE"]), 
                        "station_id":row["STATION_ID"]}
        producer.send(TOPIC_TEMP, value=(base_payload | 
                                            {"temperature":row["AIR_TEMPERATURE_200CM"],
                                             "quality":int(row["QUALITY_LEVEL"])}))
        producer.send(TOPIC_OTHE, value=(base_payload | {"pressure":row["AIR_PRESSURE"],
                                                         "humidity":row["REL_HUMIDITY"],
                                                         "dewpoint":row["DEWPOINT_TEMPERATURE"]}))
        sleep(speed)
    return

if __name__ == "__main__":
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/"
    station_names = get_station_dwd_file_storage(url, STATION_ID)
    data = get_and_unzip_files(url, station_names)
    push_records(data, calc_speed_factor(STREAM_SPEED))