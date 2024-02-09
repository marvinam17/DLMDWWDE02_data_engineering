CREATE KEYSPACE IF NOT EXISTS dwd_weather 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dwd_weather;

CREATE TABLE IF NOT EXISTS daily_temp_data(
    measurement_date timestamp,
    station_id int,
    mean_temp double,
    PRIMARY KEY(measurement_date)
);

CREATE TABLE IF NOT EXISTS daily_othe_data(
    measurement_date timestamp,
    station_id int,
    max_pressure double,
    min_pressure double,
    mean_humidity double,
    mean_dewpoint double,
    PRIMARY KEY(measurement_date)
);

CREATE TABLE IF NOT EXISTS yearly_data_temp(
    year int,
    mean_temp double,
    mean_deviation double,
    quality int,
    PRIMARY KEY(year)
);