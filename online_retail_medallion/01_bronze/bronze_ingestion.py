# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze.ny_taxi_trips AS
# MAGIC SELECT
# MAGIC   VendorID,
# MAGIC   tpep_pickup_datetime,
# MAGIC   tpep_dropoff_datetime,
# MAGIC   passenger_count,
# MAGIC   trip_distance,
# MAGIC   fare_amount,
# MAGIC   total_amount,
# MAGIC   payment_type,
# MAGIC   PULocationID,
# MAGIC   DOLocationID
# MAGIC FROM raw.yellow_taxi_2023_01_raw;