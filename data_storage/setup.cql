CREATE KEYSPACE IF NOT EXISTS dwd_weather 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dwd_weather;

CREATE TABLE IF NOT EXISTS daily_weather_data(
    measurement_date timestamp,
    station_id int,
    mean_temp double,
    std_dev_temp double,
    max_air_pressure double,
    min_air_pressure double,
    max_rel_humidity double,
    min_rel_humidity double,
    QUALITY_LEVEL int,
    PRIMARY KEY(measurement_date)
);

CREATE TABLE IF NOT EXISTS yearly_weather_data(
    measurement_year timestamp,
    max_air_pressure double,
    min_air_pressure double,
    frosty_days int,
    invalid_temp_values int,
    PRIMARY KEY(measurement_year)
);