"""
This module contains all schemas for the Streaming DataFrames.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    FloatType,
)

# Schemas for Kafka Streams and Static Dataframes
SCHEMA_TEMP = StructType(
    [
        StructField("measurement_date", StringType()),
        StructField("station_id", StringType()),
        StructField("temperature", FloatType()),
        StructField("quality", IntegerType()),
    ]
)

SCHEMA_OTHE = StructType(
    [
        StructField("measurement_date", StringType()),
        StructField("station_id", StringType()),
        StructField("pressure", FloatType()),
        StructField("humidity", FloatType()),
        StructField("dewpoint", FloatType()),
    ]
)

SCHEMA_STATIC = StructType(
    [
        StructField("station_number", IntegerType()),
        StructField("station_id", IntegerType()),
        StructField("icao_code", StringType()),
        StructField("station_name", StringType()),
        StructField("station_height", IntegerType()),
        StructField("geo_latitude", StringType()),
        StructField("geo_longitude", StringType()),
        StructField("automatic_since", TimestampType()),
        StructField("start_date", TimestampType()),
    ]
)
