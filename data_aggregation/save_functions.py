"""
This module contains all batch save functions for
Cassandra and Postgres.
"""

import os


# Functions for batch writing to CASSANDRA
def save_temp_to_cassandra(write_df, epoch_id):
    """
    Batch Writing to Cassandra in table daily_temp_data.
    """
    write_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="daily_temp_data", keyspace="dwd_weather"
    ).save()


def save_othe_to_cassandra(write_df, epoch_id):
    """
    Batch Writing to Cassandra in table daily_othe_data.
    """
    write_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="daily_othe_data", keyspace="dwd_weather"
    ).save()


def save_yearly_temp_to_cassandra(write_df, epoch_id):
    """
    Batch Writing to Cassandra in table yearly_data_temp.
    """
    write_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="yearly_data_temp", keyspace="dwd_weather"
    ).save()


def save_meta_to_postgres(write_df, epoch_id):
    """
    Batch Writing to Postgres in table dwd_weather.meta_data.
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PW")
    write_df.write.format("jdbc").option(
        "url", "jdbc:postgresql://postgres:5432/postgres"
    ).option("driver", "org.postgresql.Driver").option(
        "dbtable", "dwd_weather.meta_data"
    ).option(
        "user", user
    ).option(
        "password", password
    ).mode(
        "append"
    ).save()

def save_overall_temp_to_postgres(write_df, epoch_id):
    """
    Batch Writing to Postgres in table dwd_weather.meta_data.
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PW")
    write_df.write.format("jdbc").option(
        "url", "jdbc:postgresql://postgres:5432/postgres"
    ).option("driver", "org.postgresql.Driver").option(
        "dbtable", "dwd_weather.overall_temp"
    ).option(
        "user", user
    ).option(
        "password", password
    ).mode(
        "append"
    ).save()