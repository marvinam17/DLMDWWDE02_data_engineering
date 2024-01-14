import json
from kafka import KafkaProducer
from time import sleep
from dwd_crawling import get_and_unzip_files, get_station_dwd_file_storage

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"], 
    value_serializer=serializer
)

def push_records(data):
    for row in range(len(data)):
        payload = data.loc[row].to_dict()
        producer.send("DWD", value=payload)
        print(payload,"sent to Kafka")
        sleep(1)
    return
        
if __name__ == "__main__":
    url ="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/"
    station_names = get_station_dwd_file_storage(url, "00399")
    data = get_and_unzip_files(url, station_names)
    push_records(data)