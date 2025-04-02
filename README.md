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

```
trip_id,driver_id,distance_km,fare_amount,timestamp,event_time
c41da982-4fbc-47b8-8cf8-32b8c1912f08,49,4.9,98.02,2025-04-01 23:49:52,2025-04-01T23:49:52.000Z
f86f2bf9-e774-48b5-9e64-5fd615b7d210,42,19.18,112.28,2025-04-01 23:49:54,2025-04-01T23:49:54.000Z
```

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

```
trip_id,driver_id,distance_km,fare_amount,timestamp,event_time
77879db6-fe4f-4af8-99e3-a5b29baf389b,91,15.94,133.25,2025-04-01 23:52:33,2025-04-01 23:52:33
cc53a492-218f-4439-8607-1a985894eb6c,73,45.09,31.9,2025-04-01 23:52:35,2025-04-01 23:52:35
5ece3769-9d16-49a5-b960-94248ebea90d,71,30.66,19.43,2025-04-01 23:52:34,2025-04-01 23:52:34
```

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

```
window_start,window_end,total_fare_in_window
2025-04-02T00:57:00.000Z,2025-04-02T01:02:00.000Z,1433.83
```

### Command to Run:
```bash
spark-submit task3_time_window_analysis.py
```

---


