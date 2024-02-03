from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

SCHEMA_TEMP = StructType([
    StructField("measurement_date", StringType()),
    StructField("station_id", StringType()),
    StructField("temperature", FloatType()),
    StructField("quality", IntegerType())])

SCHEMA_PRES = StructType([
    StructField("measurement_date", StringType()),
    StructField("station_id", StringType()),
    StructField("pressure", FloatType())])

SCHEMA_HUMI = StructType([
    StructField("measurement_date", StringType()),
    StructField("station_id", StringType()),
    StructField("humidity", FloatType())])

SCHEMA_DEWP = StructType([
    StructField("measurement_date", StringType()),
    StructField("station_id", StringType()),
    StructField("dewpoint", FloatType())])

SCHEMA_STATIC = StructType([
    StructField("station_number", IntegerType()),
    StructField("station_id", IntegerType()),
    StructField("icao_code", StringType()),
    StructField("station_name", StringType()),
    StructField("station_height", IntegerType()),
    StructField("geo_latitude", StringType()),
    StructField("geo_longitude", StringType()),
    StructField("automatic_since", TimestampType()),
    StructField("start_date",TimestampType())])