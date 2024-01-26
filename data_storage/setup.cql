CREATE KEYSPACE IF NOT EXISTS dwd_weather 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE dwd_weather;

CREATE TABLE IF NOT EXISTS daily_weather(
    date timestamp,
    station_id int,
    mean_day_temp float,
    PRIMARY KEY(date)
);

TRUNCATE daily_weather;