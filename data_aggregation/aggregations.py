import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def daily_aggs_temp() -> list:
    daily_aggs_temp = [
        F.mean("temperature").alias("mean_temp"), 
    ]
    return daily_aggs_temp

def yearly_aggs_temp() -> list:
    daily_aggs_temp = [
        F.mean("temperature").alias("mean_temp"), 
        F.stddev("temperature").alias("mean_deviation"),
        F.max("quality").alias("quality")
    ]
    return daily_aggs_temp

def aggs_meta_data() -> list:
    daily_aggs_temp = [
        F.max("measurement_date").alias("max_date"),
        F.min("measurement_date").alias("min_date"),
        F.count(F.lit(1)).alias("entry_count"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature")
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

def apply_aggs(df: DataFrame, aggs:list, clean_column:list, group_columns:list) -> DataFrame:
    if len(clean_column) > 1:
        df_clean = remove_invalid_rows_multi(df, clean_column)
    else:
        df_clean = remove_invalid_rows(df, clean_column[0])
    return df_clean.groupBy(group_columns).agg(*aggs)