import os
from pyspark.sql import SparkSession
from schemas import (SCHEMA_TEMP, 
                     SCHEMA_OTHE,
                     SCHEMA_STATIC)
from utils import (create_stream,
                   apply_schema,
                   join_static_data)
from aggregations import (apply_aggs, 
                          daily_aggs_temp,
                          daily_aggs_othe,
                          yearly_aggs_temp,
                          aggs_meta_data)
from save_functions import (save_temp_to_cassandra,
                            save_othe_to_cassandra,
                            save_yearly_temp_to_cassandra,
                            save_meta_data_to_cassandra,
                            save_to_postgres)
import pyspark.sql.functions as F

CASSANDRA_USER = os.getenv("CASSANDRA_USER")
CASSANDRA_PW = os.getenv("CASSANDRA_PASSWORD")
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
CASSANDRA_KEYSPACE = "dwd_weather"
CASSANDRA_TEMP = "daily_temp_data"
CASSANDRA_OTHE = "daily_othe_data"
TOPIC_TEMP = "temperature"
TOPIC_OTHE = "others"
COL_TEMP = "temperature"
COL_PRES = "pressure"
COL_HUMI = "humidity"
COL_DEWP = "dewpoint"


if __name__ == '__main__':

    spark = SparkSession.builder.appName("weather_stream")\
    .config('spark.cassandra.connection.host', 'cassandra')\
    .config("spark.cassandra.auth.username", CASSANDRA_USER)\
    .config("spark.cassandra.auth.password", CASSANDRA_PW)\
    .getOrCreate()

    # Reduce Logging
    spark.sparkContext.setLogLevel("WARN")

    # Read Kafka Streams
    df_temp = create_stream(spark, KAFKA_BOOTSTRAP_SERVERS, TOPIC_TEMP, "earliest")
    df_othe = create_stream(spark, KAFKA_BOOTSTRAP_SERVERS, TOPIC_OTHE, "earliest")

    # Read static data
    df_static = spark.read.csv("/data/station_data.csv", 
                            header=False,
                            schema=SCHEMA_STATIC, 
                            sep=";")

    # Extract data from json in a dataframe and clean up the timestamp column
    df_temp_schema = apply_schema(df_temp, SCHEMA_TEMP)
    df_othe_schema = apply_schema(df_othe, SCHEMA_OTHE)
    df_temp_yearly = df_temp_schema.withColumn("year",F.year("measurement_date"))

    # Perform Aggregations
    df_temp_agg = apply_aggs(df_temp_schema, daily_aggs_temp(), [COL_TEMP], ["measurement_date","station_id"])
    df_othe_agg = apply_aggs(df_othe_schema, daily_aggs_othe(), [COL_PRES, COL_HUMI, COL_DEWP],["measurement_date","station_id"])
    df_temp_yearly_agg = apply_aggs(df_temp_yearly, yearly_aggs_temp(), [COL_TEMP],["year"])
    df_meta_agg = apply_aggs(df_temp_schema, aggs_meta_data(), [COL_TEMP],["station_id"])

    # Join some static data
    df_meta_joined = join_static_data(df_meta_agg, df_static)

    # Batch Queries to CASSANDRA
    query_temp = df_temp_agg.writeStream \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(save_temp_to_cassandra) \
        .outputMode("update") \
        .start()
    
    query_meta_data = df_meta_joined.writeStream \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(save_to_postgres) \
        .outputMode("complete") \
        .start()    

    query_othe = df_othe_agg.writeStream \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(save_othe_to_cassandra) \
        .outputMode("update") \
        .start()
    
    query_yearly_temp = df_temp_yearly_agg.writeStream \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(save_yearly_temp_to_cassandra) \
        .outputMode("update") \
        .start()    

    # query_yearly_temp = df_temp_yearly_agg.writeStream \
    #     .trigger(processingTime="15 seconds") \
    #     .foreachBatch(save_yearly_temp_to_cassandra) \
    #     .outputMode("update") \
    #     .start()    

    # query2 = df_temp_joined.writeStream \
    #     .trigger(processingTime="15 seconds") \
    #     .outputMode("complete") \
    #     .foreachBatch(save_to_postgres) \
    #     .start()

    # orders_agg_write_stream = df_temp_joined \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .option("checkpointLocation", "temp/spark-checkpoint-7") \
    #     .format("console") \
    #     .start()

    spark.streams.awaitAnyTermination()