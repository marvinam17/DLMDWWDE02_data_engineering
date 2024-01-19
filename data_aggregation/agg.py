from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("KafkaStreamToRDD") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "dwd-topic") \
    .option("startingOffsets", "earliest") \
    .load()

if __name__ == "__main__":

    print("test")
    
    q = df.writeStream.format(
        "console").options(numRows=3, truncate=False).start()
    time.sleep(3)
    q.stop()






spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./test.py




from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import time

spark = SparkSession.builder.appName("KafkaStreamToRDD") \
    .getOrCreate()

schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True)
])

if __name__ == "__main__":
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "dwd-topic") \
        .option("startingOffsets", "earliest") \
        .load()
    #df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    decoded_stream_df = df.select(
        col("key").cast(StringType()),
        from_json(col("value").cast("string"), schema).alias("data")
    )

    final_stream_df = decoded_stream_df.select("key", "data.*")

    output_query = final_stream_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

    # Warten Sie, bis der Streaming-Job beendet ist
    output_query.awaitTermination()
    print("-------------------------------------")
    print(type(df))
    print(df.isStreaming)
    print("-------------------------------------")
    q = df.writeStream.format("console").start()
    time.sleep(3)
    q.stop()
    print("-------------------------------------")