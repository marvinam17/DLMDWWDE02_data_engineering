import os
import json
from kafka import KafkaProducer
from time import sleep
from dwd_crawler import (get_and_unzip_files, 
                         get_station_dwd_file_storage,
                         rename_dataframe_columns,
                         reformat_timestamp)

kafka_listener = os.getenv("KAFKA_LISTENER")
streamspeed = float(os.getenv("STREAMING_SPEED"))
station_id = os.getenv("STATION_ID")

def calc_speed_factor(speed):
    """
    This is a minor reformat of the streamspeed to give the user a ground speed
    of one day per second.
    """
    streamspeed = 1/(8640*speed)
    return streamspeed

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[kafka_listener], 
    value_serializer=serializer
)

def push_records(data):
    for payload in data.to_dict(orient='records'):
        producer.send("dwd-topic", value=payload)
        sleep(calc_speed_factor(streamspeed))
    return
        
if __name__ == "__main__":
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/"
    station_names = get_station_dwd_file_storage(url, station_id)
    data = get_and_unzip_files(url, station_names)
    push_records(data)