CREATE KEYSPACE IF NOT EXISTS dwd_weather 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dwd_weather;

CREATE TABLE IF NOT EXISTS daily_weather(
    uuid uuid,
    measurement_date timestamp,
    station_id int,
    mean_day_temp double,
    PRIMARY KEY(measurement_date)
);

CREATE INDEX IF NOT EXISTS ON daily_weather (uuid);

CREATE TABLE IF NOT EXISTS all_data(
    MEASUREMENT_DATE timestamp,
    STATION_ID int,
    QUALITY_LEVEL int,
    AIR_PRESSURE double,
    AIR_TEMPERATURE_200CM double,
    AIR_TEMPERATURE_5CM double,
    REL_HUMIDITY double,
    DEWPOINT_TEMPERATURE double,
    PRIMARY KEY(measurement_date)
);