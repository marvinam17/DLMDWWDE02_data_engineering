import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def daily_aggs_temp() -> list:
    daily_aggs_temp = [
        F.mean("temperature").alias("mean_temp"), 
        F.stddev("temperature").alias("std_dev_temp"), 
        F.max("quality").alias("quality")
    ]
    return daily_aggs_temp

def daily_aggs_pres() -> list:
    daily_aggs_pres = [
        F.max("pressure").alias("max_pressure"), 
        F.min("pressure").alias("min_pressure")
    ]
    return daily_aggs_pres

def daily_aggs_humi() -> list:
    daily_aggs_humi = [
        F.max("humidity").alias("max_humidity"), 
        F.min("humidity").alias("min_humidity")
    ]
    return daily_aggs_humi

def daily_aggs_dewp() -> list:
    daily_aggs_dewp = [
        F.max("dewpoint").alias("max_dewpoint"), 
        F.min("dewpoint").alias("min_dewpoint")
    ]
    return daily_aggs_dewp

def remove_invalid_rows(df: DataFrame, column:str) -> DataFrame:
    INVALID_VALUE_MARKER = -999.0
    return df.where(F.col(column) != INVALID_VALUE_MARKER)

def apply_aggs(df: DataFrame, aggs:list, column:str) -> DataFrame:
    df_clean = remove_invalid_rows(df, column)
    return df_clean.groupBy("measurement_date","station_id").agg(*aggs)



def calc_daily_aggregations(df):
    """
    Daily aggregating each type of data separate because they sometimes have invalid values (-999.0).
    In order not to delete all information in one specific row due to one invalid value these aggregations
    are done separetly and joind afterwards.
    """
    INVALID_VALUE_MARKER = -999.0
    daily_aggs_temp = [F.mean("AIR_TEMPERATURE_200CM").alias("mean_temp"), 
                   F.stddev("AIR_TEMPERATURE_200CM").alias("std_dev_temp")]
    daily_aggs_pressure = [F.max("AIR_PRESSURE").alias("max_air_pressure"), 
                      F.min("AIR_PRESSURE").alias("min_air_pressure")] 
    daily_aggs_humidity= [F.max("REL_HUMIDITY").alias("max_rel_humidity"), 
                      F.min("REL_HUMIDITY").alias("min_rel_humidity")]
    daily_aggs_quality = [F.max("QUALITY_LEVEL").alias("quality_level")]
    df_daily = df.withColumn("measurement_date",F.to_date("MEASUREMENT_DATE"))\
                .withColumnRenamed("STATION_ID","station_id")
    df_daily_temp = df_daily.where(F.col("AIR_TEMPERATURE_200CM") != INVALID_VALUE_MARKER)\
                    .groupBy("measurement_date").agg(*daily_aggs_temp)
    df_daily_pressure = df_daily.where(F.col("AIR_PRESSURE") != INVALID_VALUE_MARKER)\
                    .groupBy("measurement_date").agg(*daily_aggs_pressure)
    df_daily_humidity = df_daily.where(F.col("REL_HUMIDITY") != INVALID_VALUE_MARKER)\
                    .groupBy("measurement_date").agg(*daily_aggs_humidity)
    df_daily_quality = df_daily.where(F.col("QUALITY_LEVEL") != INVALID_VALUE_MARKER)\
                    .groupBy("measurement_date").agg(*daily_aggs_quality)
    return df_daily.join(df_daily_temp, on="measurement_date", how="left")\
        .join(df_daily_pressure, on="measurement_date", how="left")\
        .join(df_daily_humidity, on="measurement_date", how="left")\
        .join(df_daily_quality, on="measurement_date", how="left")

def calc_yearly_aggregations(df_daily, df):
    """
    As df_daily contains already aggregated values and no invalid values all aggregations
    can be applied directly except the count of invalid values.
    """
    cnt_cond_cold_days = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    cnt_cond_invalid = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    yearly_aggs = [F.mean("mean_temp").alias("mean_temp"), 
               F.max("max_air_pressure").alias("max_air_pressure"), 
               F.min("min_air_pressure").alias("min_air_pressure"),
               cnt_cond_cold_days(F.col("mean_temp") < 0.0).alias("frosty_days")]

    df_yearly = df_daily.withColumn("measurement_year",F.year("measurement_date"))
    df_yearly_agg = df_yearly.groupBy("measurement_year").agg(*yearly_aggs)
    df_yearly_agg_invalid_temp_values = df.withColumn("measurement_year",F.year("MEASUREMENT_DATE")) \
            .groupBy("measurement_year")\
            .agg(cnt_cond_invalid(F.col("AIR_TEMPERATURE_200CM")==-999.0).alias("invalid_temp_values"))
    return df_yearly_agg.join(
        df_yearly_agg_invalid_temp_values, on = "measurement_year", how = "left")


