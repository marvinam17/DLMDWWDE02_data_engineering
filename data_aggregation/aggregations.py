import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def daily_aggs_temp() -> list:
    daily_aggs_temp = [
        F.mean("temperature").alias("mean_temp"), 
        F.stddev("temperature").alias("std_dev_temp"), 
        F.max("quality").alias("quality")
    ]
    return daily_aggs_temp

def daily_aggs_othe() -> list:
    daily_aggs_othe = [
        F.max("pressure").alias("max_pressure"), 
        F.min("pressure").alias("min_pressure"),
        F.mean("humidity").alias("mean_humidity"), 
        F.mean("dewpoint").alias("mean_dewpoint"), 
    ]
    return daily_aggs_othe

def remove_invalid_rows(df: DataFrame, column:str) -> DataFrame:
    INVALID_VALUE_MARKER = -999.0
    return df.where(F.col(column) != INVALID_VALUE_MARKER)

def remove_invalid_rows_multi(df: DataFrame, column:list) -> DataFrame:
    INVALID_VALUE_MARKER = -999.0
    for col in column:
        df = df.where(F.col(col) != INVALID_VALUE_MARKER)
    return df

def apply_aggs(df: DataFrame, aggs:list, column:list) -> DataFrame:
    if len(column) > 1:
        df_clean = remove_invalid_rows_multi(df, column)
    else:
        df_clean = remove_invalid_rows(df, column[0])
    return df_clean.groupBy("measurement_date","station_id").agg(*aggs)