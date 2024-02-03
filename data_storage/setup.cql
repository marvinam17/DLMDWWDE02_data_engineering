CREATE KEYSPACE IF NOT EXISTS dwd_weather 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dwd_weather;

CREATE TABLE IF NOT EXISTS daily_temp_data(
    measurement_date timestamp,
    station_id int,
    mean_temp double,
    std_dev_temp double,
    quality int,
    icao_code varchar,
    station_name varchar,
    station_height int,
    PRIMARY KEY(measurement_date)
);

CREATE TABLE IF NOT EXISTS daily_pres_data(
    measurement_date timestamp,
    station_id int,
    max_pressure double,
    min_pressure double,
    PRIMARY KEY(measurement_date)
);

CREATE TABLE IF NOT EXISTS daily_humi_data(
    measurement_date timestamp,
    station_id int,
    max_humidity double,
    min_humidity double,
    PRIMARY KEY(measurement_date)
);

CREATE TABLE IF NOT EXISTS daily_dewp_data(
    measurement_date timestamp,
    station_id int,
    max_dewpoint double,
    min_dewpoint double,
    PRIMARY KEY(measurement_date)
);

