from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# SCHEMA_BASE = StructType([
#     StructField("station_id", IntegerType()),
#     StructField("measurement_date", StringType())])

# SCHEMA_TEMP = SCHEMA_BASE.add(
#     StructField("temperature", FloatType())).add(
#     StructField("quality", IntegerType()))

# SCHEMA_PRES = SCHEMA_BASE.add(
#     StructField("pressure", FloatType()))

# SCHEMA_HUMI = SCHEMA_BASE.add(
#     StructField("humidity", FloatType()))

# SCHEMA_DEWP = SCHEMA_BASE.add(
#     StructField("dewpoint", FloatType()))

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