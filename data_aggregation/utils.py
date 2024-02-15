"""
This module contains some basic functions for the Streaming application.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def create_stream(spark: SparkSession, servers: list, topic: str, offset: str) -> DataFrame:
    """
    Untested, as this function is strongly dependend on the Kafka Producer Spec
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topic)
        .option("startingOffsets", offset)
        .option("failOnDataLoss", "false")
        .load()
    )


def convert_to_timestamp(df: DataFrame) -> DataFrame:
    """
    This function converts a string column that contains a timestamp in the format
    yyyyMMddHHmm in a timestamp column. Column name is measurement_date

    >>> spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    >>> df = spark.createDataFrame([("199912121222",), ("202402111111",)], ["measurement_date"])
    >>> result = convert_to_timestamp(df)
    >>> result.show()
    +-------------------+
    |   measurement_date|
    +-------------------+
    |1999-12-12 12:22:00|
    |2024-02-11 11:11:00|
    +-------------------+
    <BLANKLINE>
    """
    return df.withColumn(
        "measurement_date",
        F.to_timestamp("measurement_date", "yyyyMMddHHmm").cast("timestamp"),
    )


def apply_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    This function takes a Spark Streaming df from Kafka and a Schema.
    It returns a Dataframe with the given Schema. Furthermore, the
    measurement_date column is converted from timestamp to date.

    >>> from pyspark.sql.types import (StructType, StructField, StringType,IntegerType, TimestampType, FloatType)
    >>> spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    >>> SCHEMA = StructType([StructField("measurement_date", StringType()),StructField("station_id", StringType()),StructField("temperature", FloatType()),StructField("quality", IntegerType())])
    >>> df = spark.createDataFrame([(b'{"measurement_date":"199902011511","station_id":"00433","temperature":20.0,"quality":1}',)], ["value"])
    >>> result = apply_schema(df, SCHEMA)
    >>> result.show()
    +----------------+----------+-----------+-------+
    |measurement_date|station_id|temperature|quality|
    +----------------+----------+-----------+-------+
    |      1999-02-01|       433|       20.0|      1|
    +----------------+----------+-----------+-------+
    <BLANKLINE>
    """
    json_data = (
        df.selectExpr("CAST(value AS STRING)")
        .select(F.from_json("value", schema).alias("data"))
        .select("data.*")
    )
    df_extracted = convert_to_timestamp(json_data)
    return df_extracted.withColumn(
        "measurement_date", F.to_date("measurement_date")
    ).withColumn("station_id", F.col("station_id").cast("integer"))


def join_static_data(df: DataFrame, df_static: DataFrame) -> DataFrame:
    """
    This function takes a Streaming Dataframe and a Static Dataframe. It applies
    a left join of the static Dataframe to the Streaming Dataframe on the station_id column.

    >>> spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    >>> df = spark.createDataFrame([("00433",)], ["station_id"])
    >>> df_static = spark.createDataFrame([("00433","ETNN","example",50),("00599","ETNL","example_2",52)],["station_id","icao_code","station_name","station_height"])
    >>> result = join_static_data(df, df_static)
    >>> result.show()
    +----------+---------+------------+--------------+
    |station_id|icao_code|station_name|station_height|
    +----------+---------+------------+--------------+
    |     00433|     ETNN|     example|            50|
    +----------+---------+------------+--------------+
    <BLANKLINE>
    """
    df_static_selected = df_static.select(
        "station_id", "icao_code", "station_name", "station_height"
    )
    return df.join(df_static_selected, on="station_id", how="left")
