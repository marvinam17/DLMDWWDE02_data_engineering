import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

CASSANDRA_USER = "cassandra"
CASSANDRA_PW = os.getenv("CASSANDRA_PASSWORD")
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "dwd-topic"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

SCHEMA = StructType([
    StructField("STATION_ID", IntegerType()),
    StructField("MEASUREMENT_DATE", StringType()),
    StructField("QUALITY_LEVEL", IntegerType()),
    StructField("AIR_PRESSURE", FloatType()),
    StructField("AIR_TEMPERATURE_200CM", FloatType()),
    StructField("AIR_TEMPERATURE_5CM", FloatType()),
    StructField("REL_HUMIDITY", FloatType()),
    StructField("DEWPOINT_TEMPERATURE", FloatType()),
])

spark = SparkSession.builder.appName("weather_stream")\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config('spark.cassandra.connection.host', 'cassandra')\
    .config("spark.cassandra.auth.username", CASSANDRA_USER)\
    .config("spark.cassandra.auth.password", CASSANDRA_PW)\
    .getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

#!!!!!!!!!!!!!!!!!!!!!!!!!!
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()
# set earliest here!!!!!!!!!!


# JSON-Daten aus dem Kafka-Stream extrahieren und das Schema anwenden
json_data = df.selectExpr("CAST(value AS STRING)").select(F.from_json("value", SCHEMA).alias("data"))
kafka_data_df = json_data.select("data.*")
kafka_data_df_agg = kafka_data_df.withColumn("MEASUREMENT_DATE",F.to_timestamp("MEASUREMENT_DATE","yyyyMMddHHmm").cast("timestamp"))\
    .withColumn("measurement_date",F.to_date("MEASUREMENT_DATE"))\
    .where(F.col("AIR_TEMPERATURE_200CM") != "-999.0")\
    .withColumnRenamed("STATION_ID","station_id")\
    .groupby("measurement_date", "station_id").agg(F.mean("AIR_TEMPERATURE_200CM").alias("mean_day_temp"))

#DataFrame anzeigen (kann auch in eine andere Datenquelle geschrieben werden)
# query = kafka_data_df \
#     .writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .start()\
#     .awaitTermination()

table_name = "daily_weather"
keyspace = "dwd_weather"

# kafka_data_df.writeStream \
#     .format("org.apache.spark.sql.cassandra") \
#     .outputMode('append') \
#     .options(table=table_name, keyspace=keyspace)\
#     .start() \
#     .awaitTermination()


def save_to_cassandra(writeDF, epoch_id):
  
    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=table_name, keyspace=keyspace)\
        .save()
    
def save_to_cassandra_2(writeDF, epoch_id):
  
    writeDF.write \
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="all_data", keyspace=keyspace)\
        .save()

query1 = kafka_data_df_agg.writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(save_to_cassandra) \
    .outputMode("update") \
    .start()

query2 = kafka_data_df.withColumn("measurement_date",F.to_timestamp("MEASUREMENT_DATE","yyyyMMddHHmm").cast("timestamp")) \
    .select([F.col(x).alias(x.lower()) for x in kafka_data_df.columns]) \
    .writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(save_to_cassandra_2) \
    .outputMode("update") \
    .start()

# orders_agg_write_stream = kafka_data_df \
#         .writeStream \
#         .trigger(processingTime='5 seconds') \
#         .outputMode("update") \
#         .option("truncate", "false")\
#         .option("checkpointLocation", "temp/spark-checkpoint") \
#         .format("console") \
#         .start()

spark.streams.awaitAnyTermination()