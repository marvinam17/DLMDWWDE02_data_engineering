import os
import json
from kafka import KafkaProducer
from time import sleep
from pandas import DataFrame
from dwd_crawler import (get_and_unzip_files, 
                         get_station_dwd_file_storage)

kafka_listener = os.getenv("KAFKA_LISTENER")
streamspeed = float(os.getenv("STREAMING_SPEED"))
station_id = os.getenv("STATION_ID")

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
    bootstrap_servers=[kafka_listener], 
    value_serializer=serializer
)

def push_records(data:DataFrame):
    # for payload in data.to_dict(orient='records'):
    #     print(payload)
    for _, row in data.iterrows():
        base_payload = {"measurement_date":int(row["MEASUREMENT_DATE"]), 
                        "station_id":row["STATION_ID"]}
        producer.send("temperature", value=(base_payload | 
                                            {"temperature":row["AIR_TEMPERATURE_200CM"],
                                             "quality":int(row["QUALITY_LEVEL"])}))
        producer.send("pressure", value=(base_payload | {"pressure":row["AIR_PRESSURE"]}))
        producer.send("humidity", value=(base_payload | {"humidity":row["REL_HUMIDITY"]}))
        producer.send("dewpoint", value=(base_payload | {"dewpoint":row["DEWPOINT_TEMPERATURE"]}))
        sleep(calc_speed_factor(streamspeed))
    return

if __name__ == "__main__":
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/"
    station_names = get_station_dwd_file_storage(url, station_id)
    data = get_and_unzip_files(url, station_names)
    push_records(data)