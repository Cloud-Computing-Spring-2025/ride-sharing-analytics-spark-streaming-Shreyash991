import os  # Add this import for directory creation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark session
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

# ✅ Add Watermark for Aggregations
parsed_ride_stream = parsed_ride_stream.withWatermark("event_time", "10 minutes")

# Define a function to process each micro-batch
def save_batch_to_csv(batch_df, batch_id):
    output_dir = "output/aggregated"
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    file_path = f"{output_dir}/taxi_data_batch_{batch_id}.csv"
    batch_df.toPandas().to_csv(file_path, mode="w", index=False, header=True)
    print(f"Saved batch {batch_id} to {file_path}")

# Write the stream using foreachBatch
ride_query = parsed_ride_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(save_batch_to_csv) \
    .option("path", "output/aggregated/") \
    .start()

ride_query.awaitTermination()