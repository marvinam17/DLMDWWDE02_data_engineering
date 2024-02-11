CREATE SCHEMA IF NOT EXISTS dwd_weather;
    CREATE TABLE IF NOT EXISTS dwd_weather.meta_data(
        b_id SERIAL PRIMARY KEY,
        station_id integer,
        max_date timestamp,
        min_date timestamp,
        entry_count integer,
        icao_code varchar,
        station_name varchar,
        station_height integer,
        min_temperature double precision,
        max_temperature double precision
    );