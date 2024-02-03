from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def create_stream(spark: SparkSession, servers:list, topic:str, offset:str):
    return  spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", offset) \
    .option("failOnDataLoss", "false") \
    .load()

def convert_to_timestamp(df:DataFrame) -> DataFrame:
    return df.withColumn("measurement_date",
            F.to_timestamp("measurement_date","yyyyMMddHHmm")\
            .cast("timestamp"))

def apply_schema(df: DataFrame, schema:StructType) -> DataFrame:
    json_data = df.selectExpr("CAST(value AS STRING)")\
        .select(F.from_json("value",schema).alias("data")).select("data.*")
    df_extracted = convert_to_timestamp(json_data)
    return df_extracted.withColumn("measurement_date",
            F.to_date("measurement_date"))\
            .withColumn("station_id",F.col("station_id").cast("integer"))


def join_static_data(df: DataFrame, df_static: DataFrame) -> DataFrame:
    df_static_selected = df_static.select("station_id",
                                        "icao_code",
                                        "station_name",
                                        "station_height")
    return df.join(df_static_selected,
            on="station_id",
            how="left")