import os
from pyspark.sql import SparkSession
from schemas import (SCHEMA_TEMP, 
                     SCHEMA_PRES, 
                     SCHEMA_HUMI, 
                     SCHEMA_DEWP, 
                     SCHEMA_STATIC)
from utils import (create_stream,
                   apply_schema,
                   join_static_data)
from aggregations import (apply_aggs, 
                          daily_aggs_temp,
                          daily_aggs_pres,
                          daily_aggs_humi,
                          daily_aggs_dewp)

CASSANDRA_USER = "cassandra"
CASSANDRA_PW = os.getenv("CASSANDRA_PASSWORD")
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "dwd-topic"
CASSANDRA_KEYSPACE = "dwd_weather"
CASSANDRA_TEMP = "daily_temp_data"
CASSANDRA_PRES = "daily_pres_data"
CASSANDRA_HUMI = "daily_humi_data"
CASSANDRA_DEWP = "daily_dewp_data"
COL_TEMP = "temperature"
COL_PRES = "pressure"
COL_HUMI = "humidity"
COL_DEWP = "dewpoint"

# Functions for batch writing to CASSANDRA
def save_temp_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table=CASSANDRA_TEMP, keyspace=CASSANDRA_KEYSPACE)\
    .save()

def save_pres_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table=CASSANDRA_PRES, keyspace=CASSANDRA_KEYSPACE)\
    .save()

def save_humi_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table=CASSANDRA_HUMI, keyspace=CASSANDRA_KEYSPACE)\
    .save()

def save_dewp_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table=CASSANDRA_DEWP, keyspace=CASSANDRA_KEYSPACE)\
    .save()

if __name__ == '__main__':

    # Build Spark Session
    spark = SparkSession.builder.appName("weather_stream")\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .config('spark.cassandra.connection.host', 'cassandra')\
    .config("spark.cassandra.auth.username", CASSANDRA_USER)\
    .config("spark.cassandra.auth.password", CASSANDRA_PW)\
    .getOrCreate()

    # Reduce Logging
    spark.sparkContext.setLogLevel("WARN")

    # Read Kafka Streams
    df_temp = create_stream(spark, KAFKA_BOOTSTRAP_SERVERS, COL_TEMP, "earliest")
    df_pres = create_stream(spark, KAFKA_BOOTSTRAP_SERVERS, COL_PRES, "earliest")
    df_humi = create_stream(spark, KAFKA_BOOTSTRAP_SERVERS, COL_HUMI, "earliest")
    df_dewp = create_stream(spark, KAFKA_BOOTSTRAP_SERVERS, COL_DEWP, "earliest")

    # Read static data
    df_static = spark.read.csv("/data/station_data.csv", 
                            header=False,
                            schema=SCHEMA_STATIC, 
                            sep=";")

    # Extract data from json in a dataframe and clean up the timestamp column
    df_temp_schema = apply_schema(df_temp, SCHEMA_TEMP)
    df_pres_schema = apply_schema(df_pres, SCHEMA_PRES)
    df_humi_schema = apply_schema(df_humi, SCHEMA_HUMI)
    df_dewp_schema = apply_schema(df_dewp, SCHEMA_DEWP)

    # Perform Aggregations
    df_temp_agg = apply_aggs(df_temp_schema, daily_aggs_temp(), COL_TEMP)
    df_pres_agg = apply_aggs(df_pres_schema, daily_aggs_pres(), COL_PRES)
    df_humi_agg = apply_aggs(df_humi_schema, daily_aggs_humi(), COL_HUMI)
    df_dewp_agg = apply_aggs(df_dewp_schema, daily_aggs_dewp(), COL_DEWP)

    # Join some static data
    df_temp_joined = join_static_data(df_temp_agg, df_static)

    # Batch Queries to CASSANDRA
    query_temp = df_temp_joined.writeStream \
        .trigger(processingTime="5 seconds") \
        .foreachBatch(save_temp_to_cassandra) \
        .outputMode("update") \
        .start()

    query_pres = df_pres_agg.writeStream \
        .trigger(processingTime="5 seconds") \
        .foreachBatch(save_pres_to_cassandra) \
        .outputMode("update") \
        .start()

    query_humi = df_humi_agg.writeStream \
        .trigger(processingTime="5 seconds") \
        .foreachBatch(save_humi_to_cassandra) \
        .outputMode("update") \
        .start()

    query_dewp = df_dewp_agg.writeStream \
        .trigger(processingTime="5 seconds") \
        .foreachBatch(save_dewp_to_cassandra) \
        .outputMode("update") \
        .start()

    # orders_agg_write_stream = df_temp_joined \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .option("checkpointLocation", "temp/spark-checkpoint-7") \
    #     .format("console") \
    #     .start()

    spark.streams.awaitAnyTermination()