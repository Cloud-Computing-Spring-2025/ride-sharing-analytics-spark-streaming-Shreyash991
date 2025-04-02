# Real-Time Ride-Sharing Analytics with Apache Spark

In this project, you will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. The pipeline processes streaming data, performs real-time aggregations, and analyzes trends over time.

## Workflow Overview

1. **Task 1**: Ingest and parse real-time ride data.
2. **Task 2**: Perform real-time aggregations on driver earnings and trip distances.
3. **Task 3**: Analyze trends over time using a sliding time window.

---

## Task 1: Basic Streaming Ingestion and Parsing

### Objective:
Ingest streaming data from a socket and parse JSON messages into a Spark DataFrame.

### Steps:
1. Create a Spark session.
2. Use `spark.readStream.format("socket")` to read from `localhost:9999`.
3. Define a schema for the incoming JSON data.
4. Parse the JSON payload into columns.
5. Write the parsed data to a CSV file.

### Sample Output:
Parsed data is saved to `output/parsed_stream/`.

### Command to Run:
```bash
spark-submit task1_data_ingestion.py
```

---

## Task 2: Real-Time Aggregations (Driver-Level)

### Objective:
Aggregate data in real time to compute:
- Total fare amount grouped by `driver_id`.
- Average distance grouped by `driver_id`.

### Steps:
1. Reuse the parsed DataFrame from Task 1.
2. Add a watermark to handle late data.
3. Group by `driver_id` and compute:
   - `SUM(fare_amount)` as `total_fare`.
   - `AVG(distance_km)` as `avg_distance`.
4. Write the aggregated results to CSV files.

### Sample Output:
Aggregated data is saved to `output/aggregated/`.

### Command to Run:
```bash
spark-submit task2_driver_analytics.py
```

---

## Task 3: Windowed Time-Based Analytics

### Objective:
Perform a 5-minute windowed aggregation on `fare_amount` (sliding by 1 minute).

### Steps:
1. Convert the `timestamp` column to `TimestampType`.
2. Add a watermark to handle late data.
3. Use Spark’s `window` function to aggregate over a 5-minute window.
4. Write the windowed results to CSV files.

### Sample Output:
Windowed data is saved to `output/window/`.

### Command to Run:
```bash
spark-submit task3_time_window_analysis.py
```

---

## Summary

By completing this project, you will gain hands-on experience in real-time data streaming and processing using Apache Spark Structured Streaming.
