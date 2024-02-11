import os

# Functions for batch writing to CASSANDRA
def save_temp_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="daily_temp_data", keyspace="dwd_weather")\
    .save()

def save_othe_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="daily_othe_data", keyspace="dwd_weather")\
    .save()

def save_yearly_temp_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="yearly_data_temp", keyspace="dwd_weather")\
    .save()

def save_meta_data_to_cassandra(writeDF, epoch_id):
    writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="meta_data", keyspace="dwd_weather")\
    .save()

def save_to_postgres(writeDF, epoch_id):
  user = os.getenv("POSTGRES_USER")
  password = os.getenv("POSTGRES_PW")
  writeDF.write \
    .format("jdbc")\
    .option("url","jdbc:postgresql://postgres:5432/postgres")\
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable","dwd_weather.meta_data")\
    .option("user",user)\
    .option("password",password)\
    .mode("append")\
    .save()

def save_yearly_temp_to_postgres(writeDF, epoch_id):
  writeDF.write \
    .format("jdbc")\
    .option("url","jdbc:postgresql://postgres:5432/postgres")\
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable","dwd_weather.yearly_data_temp")\
    .option("user","postgres")\
    .option("password","postgres")\
    .mode("append")\
    .save()
