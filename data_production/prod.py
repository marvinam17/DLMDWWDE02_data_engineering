import json
from kafka import KafkaProducer
from time import sleep
from dwd_crawling import get_and_unzip_files, get_station_dwd_file_storage

def serializer(message):
    return json.dumps(message).encode('utf-8')

# producer = KafkaProducer(
#     bootstrap_servers=["localhost:9092"], 
#     value_serializer=serializer
# )

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

def push_records(data):
    for payload in data.to_dict(orient='records'):
        producer.send("dwd-topic", value=payload)
        sleep(0.1)
    return
        
if __name__ == "__main__":
    url ="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/"
    station_names = get_station_dwd_file_storage(url, "00399")
    data = get_and_unzip_files(url, station_names)
    push_records(data)