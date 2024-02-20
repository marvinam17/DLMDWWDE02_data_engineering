"""
This module provides the Kafka Producer that send weather data to Kafka.
"""

import os
import json
from time import sleep
from kafka import KafkaProducer
import pandas as pd
from dwd_crawler import get_and_unzip_files, get_station_dwd_file_storage

URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/"
KAFKA_LISTENER = os.getenv("KAFKA_LISTENER")
STREAM_SPEED = float(os.getenv("STREAMING_SPEED"))
STATION_ID = os.getenv("STATION_ID")
TOPIC_TEMP = "temperature"
TOPIC_OTHE = "others"


def calc_speed_factor(speed: float):
    """
    This is a minor reformat of the streamspeed to give the user a theoretical ground speed
    of one day per second if the speed is the to 1.0
    """
    streamspeed = 1 / (144 * speed)
    return streamspeed


def serializer(message):
    return json.dumps(message).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_LISTENER], value_serializer=serializer
)


def push_records(data: pd.DataFrame, speed: float):
    for _, row in data.iterrows():
        base_payload = {
            "measurement_date": int(row["MEASUREMENT_DATE"]),
            "station_id": row["STATION_ID"],
        }
        producer.send(
            TOPIC_TEMP,
            value=(
                base_payload
                | {
                    "temperature": row["AIR_TEMPERATURE_200CM"],
                    "quality": int(row["QUALITY_LEVEL"]),
                }
            ),
        )
        producer.send(
            TOPIC_OTHE,
            value=(
                base_payload
                | {
                    "pressure": row["AIR_PRESSURE"],
                    "humidity": row["REL_HUMIDITY"],
                    "dewpoint": row["DEWPOINT_TEMPERATURE"],
                }
            ),
        )
        sleep(speed)
    return


if __name__ == "__main__":
    try:
        # Always try to get the most actual data.
        station_names = get_station_dwd_file_storage(URL, STATION_ID)
        data = get_and_unzip_files(URL, station_names)
    except:
        # Take local data if actual data is not available due to missing 
        # Internet connection or changes in DWD Storage.
        data = pd.read_csv("sample_data/dwd_00433.csv", sep=";")
    push_records(data, calc_speed_factor(STREAM_SPEED))
