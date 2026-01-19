# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.session_metrics AS
# MAGIC SELECT
# MAGIC   session_id,
# MAGIC   vendor_id,
# MAGIC
# MAGIC   MIN(pickup_ts)  AS session_start_ts,
# MAGIC   MAX(dropoff_ts) AS session_end_ts,
# MAGIC   DATEDIFF(
# MAGIC   SECOND,
# MAGIC   MIN(pickup_ts),
# MAGIC   MAX(dropoff_ts)
# MAGIC ) AS session_duration_seconds,
# MAGIC
# MAGIC   COUNT(*) AS trip_count,
# MAGIC   SUM(COALESCE(total_amount, 0)) AS total_revenue,
# MAGIC   SUM(COALESCE(trip_distance, 0)) AS total_distance,
# MAGIC
# MAGIC   AVG(COALESCE(total_amount, 0)) AS avg_trip_revenue,
# MAGIC   AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance
# MAGIC
# MAGIC FROM silver.sessions
# MAGIC GROUP BY 1,2;