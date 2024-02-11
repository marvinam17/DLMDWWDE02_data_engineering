"""
This module containes all functions that are necessary to
perform aggregations.
"""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def daily_aggs_temp() -> list:
    """
    Return Daily Temperature Aggregations
    """
    aggs = [
        F.mean("temperature").alias("mean_temp"),
    ]
    return aggs

def yearly_aggs_temp() -> list:
    """
    Return Yearly Temperature and Quality Aggregations
    """
    aggs = [
        F.mean("temperature").alias("mean_temp"),
        F.stddev("temperature").alias("mean_deviation"),
        F.max("quality").alias("quality")
    ]
    return aggs

def aggs_meta_data() -> list:
    """
    Return MetaData Aggregations
    """
    aggs = [
        F.max("measurement_date").alias("max_date"),
        F.min("measurement_date").alias("min_date"),
        F.count(F.lit(1)).alias("entry_count"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature")
    ]
    return aggs

def daily_aggs_othe() -> list:
    """
    Return Daily Others Aggregations
    """
    aggs = [
        F.max("pressure").alias("max_pressure"),
        F.min("pressure").alias("min_pressure"),
        F.mean("humidity").alias("mean_humidity"),
        F.mean("dewpoint").alias("mean_dewpoint"),
    ]
    return aggs

def remove_invalid_rows(df: DataFrame, column:str) -> DataFrame:
    """
    This function takes a dataframe and a column name. It removes all rows with an invalid
    value of "-999.0" and returns the resulting Dataframe.
    
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    >>> df = spark.createDataFrame([(-999.0,), (20.0,)], ["example_column"])
    >>> result = remove_invalid_rows(df, "example_column")
    >>> result.show()    
    +--------------+
    |example_column|
    +--------------+
    |          20.0|
    +--------------+
    <BLANKLINE>
    """
    invalid_marker = -999.0
    return df.where(F.col(column) != invalid_marker)

def remove_invalid_rows_multi(df: DataFrame, column:list) -> DataFrame:
    """
    This function takes a dataframe and a list of column names. It removes all rows with an invalid
    value of "-999.0" in the listed columns and returns the resulting Dataframe.
    
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    >>> df = spark.createDataFrame([(-999.0,20.0), (20.0,30.0), (20.0,-999.0)], 
    ["example_column","second_column"])
    >>> result = remove_invalid_rows_multi(df, ["example_column","second_column"])
    >>> result.show()    
    +--------------+-------------+
    |example_column|second_column|
    +--------------+-------------+
    |          20.0|         30.0|
    +--------------+-------------+
    <BLANKLINE>
    """
    invalid_marker = -999.0
    for col in column:
        df = df.where(F.col(col) != invalid_marker)
    return df

def apply_aggs(df: DataFrame, aggs:list, clean_column:list, group_columns:list) -> DataFrame:
    """
    This function takes a dataframe, a list of column names to remove invalid rows,
    a list of columns to groupBy and a list of aggregation functions. It returns
    a cleaned and aggregated Dataframe. 
    
    >>> from pyspark.sql import SparkSession
    >>> import pyspark.sql.functions as F
    >>> spark = SparkSession.builder.master("local[2]").appName("Test").getOrCreate()
    >>> df = spark.createDataFrame([("1999-02-01",-999.0,20.0,), ("1999-02-01", 20.0,30.0,), 
    ("1999-02-01", 20.0,-999.0,), ("1999-02-01", 30.0,20.0,)], 
    ["measurement_date","example_column","second_column"])
    >>> aggs = [F.mean("example_column").alias("mean_example")]
    >>> result = apply_aggs(df, aggs,["example_column","second_column"],["measurement_date"])
    >>> result.show()    
    +----------------+------------+
    |measurement_date|mean_example|
    +----------------+------------+
    |      1999-02-01|        25.0|
    +----------------+------------+
    <BLANKLINE>
    """
    if len(clean_column) > 1:
        df_clean = remove_invalid_rows_multi(df, clean_column)
    else:
        df_clean = remove_invalid_rows(df, clean_column[0])
    return df_clean.groupBy(group_columns).agg(*aggs)
