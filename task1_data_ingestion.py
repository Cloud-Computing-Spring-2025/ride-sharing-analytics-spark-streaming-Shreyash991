from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum, avg, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Initialize Spark Session
spark_session = SparkSession.builder.appName("StreamingTaxiData").getOrCreate()

# Define schema for incoming JSON data
ride_data_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
ride_stream = spark_session.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse JSON data
parsed_ride_stream = ride_stream.select(from_json(col("value"), ride_data_schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType
parsed_ride_stream = parsed_ride_stream.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Task 1: Print parsed data to console
ride_query = parsed_ride_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/parsed_stream/") \
    .option("checkpointLocation", "output/checkpoints/parsed_stream/") \
    .option("header", "true") \
    .start()

# Await termination
ride_query.awaitTermination()