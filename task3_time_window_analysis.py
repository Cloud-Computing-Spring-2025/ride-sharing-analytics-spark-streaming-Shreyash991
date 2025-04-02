from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Create Spark Session
spark = SparkSession.builder \
    .appName("RideSharing-TimeWindowAnalysis") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

# Define schema for the incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Connect to socket stream
stream_data = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data
parsed_data = stream_data \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp string to proper TimestampType
data_with_timestamp = parsed_data \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

# Perform 5-minute window aggregation with 1-minute sliding interval
windowed_aggregation = data_with_timestamp \
    .groupBy(window("event_time", "5 minutes", "1 minute")) \
    .agg(sum("fare_amount").alias("total_fare_in_window"))

# Select relevant columns for output
output_df = windowed_aggregation \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare_in_window")
    )

# Define function to process each batch
def process_batch_and_write_to_csv(df, batch_id):
    # Write the current complete result to CSV
    if not df.isEmpty():
        # Add batch_id to filename to make each output unique
        output_path = f"output/windowed_results/batch_{batch_id}"
        df.write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"Batch {batch_id} written to {output_path}")

# Write the windowed aggregation results using foreachBatch
query = output_df \
    .writeStream \
    .foreachBatch(process_batch_and_write_to_csv) \
    .outputMode("complete") \
    .option("checkpointLocation", "checkpoint/windowed_results") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
