# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.trip_metrics AS
# MAGIC SELECT
# MAGIC   date_trunc('day', pickup_ts) AS trip_date,
# MAGIC   COUNT(*) AS trips,
# MAGIC   AVG(trip_distance) AS avg_distance,
# MAGIC   AVG(trip_minutes) AS avg_duration,
# MAGIC   SUM(total_amount) AS total_revenue
# MAGIC FROM silver.trips
# MAGIC GROUP BY 1;