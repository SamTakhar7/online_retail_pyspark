# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.trips AS
# MAGIC SELECT
# MAGIC   VendorID,
# MAGIC   CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_ts,
# MAGIC   CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
# MAGIC   passenger_count,
# MAGIC   trip_distance,
# MAGIC   fare_amount,
# MAGIC   total_amount,
# MAGIC   (unix_timestamp(tpep_dropoff_datetime) 
# MAGIC    - unix_timestamp(tpep_pickup_datetime)) / 60 AS trip_minutes
# MAGIC FROM bronze.ny_taxi_trips
# MAGIC WHERE trip_distance > 0
# MAGIC   AND total_amount > 0;