import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from schemas import SCHEMA_TEMP, SCHEMA_PRES, SCHEMA_HUMI, SCHEMA_DEWP
from aggregations import calc_daily_aggregations, calc_yearly_aggregations
from pyspark.sql.types import StructType


CASSANDRA_USER = "cassandra"
CASSANDRA_PW = os.getenv("CASSANDRA_PASSWORD")
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "dwd-topic"
CASSANDRA_KEYSPACE = "dwd_weather"
CASSANDRA_TABLE_DAILY = "daily_weather_data"
CASSANDRA_TABLE_YEARLY = "yearly_weather_data"

# Build Spark session
spark = SparkSession.builder.appName("weather_stream")\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config('spark.cassandra.connection.host', 'cassandra')\
    .config("spark.cassandra.auth.username", CASSANDRA_USER)\
    .config("spark.cassandra.auth.password", CASSANDRA_PW)\
    .getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

def create_stream(topic:str, offset:str):
    return  spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", topic) \
    .option("startingOffsets", offset) \
    .option("failOnDataLoss", "false") \
    .load()

def apply_schema(df: DataFrame, schema:StructType):
    json_data = df.selectExpr("CAST(value AS STRING)")\
        .select(F.from_json("value",schema).alias("data"))
    return json_data.select("data.*")\
        .withColumn("measurement_date",
            F.to_timestamp("measurement_date","yyyyMMddHHmm")\
            .cast("timestamp"))

# Read Kafka Streams
df_temp = create_stream("temperature", "earliest")
df_pres = create_stream("pressure", "earliest")
df_humi = create_stream("humidity", "earliest")
df_dewp = create_stream("dewpoint", "earliest")

# Extract data from json in a dataframe and clean up the timestamp column
df_temp_schema = apply_schema(df_temp, SCHEMA_TEMP)
df_pres_schema = apply_schema(df_pres, SCHEMA_PRES)
df_humi_schema = apply_schema(df_humi, SCHEMA_HUMI)
df_dewp_schema = apply_schema(df_dewp, SCHEMA_DEWP)

# Hier weiter


# # Calculate the aggregations
#df_daily_aggregated = calc_daily_aggregations(df_cleaned)
#df_yearly_aggregated = calc_yearly_aggregations(df_daily_aggregated, df_cleaned)

# Functions for batch writing to CASSANDRA
def save_daily_to_cassandra(writeDF, epoch_id):
    writeDF = calc_daily_aggregations(writeDF)
    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=CASSANDRA_TABLE_DAILY, keyspace=CASSANDRA_KEYSPACE)\
        .save()
    
def save_yearly_to_cassandra(writeDF, epoch_id):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=CASSANDRA_TABLE_YEARLY, keyspace=CASSANDRA_KEYSPACE)\
        .save()

# # Batch Queries to CASSANDRA DB
# query_daily = df_cleaned.writeStream \
#     .trigger(processingTime="5 seconds") \
#     .foreachBatch(save_daily_to_cassandra) \
#     .outputMode("update") \
#     .start()

orders_agg_write_stream = df_temp_schema \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .option("checkpointLocation", "temp/spark-checkpoint") \
        .format("console") \
        .start()

spark.streams.awaitAnyTermination()