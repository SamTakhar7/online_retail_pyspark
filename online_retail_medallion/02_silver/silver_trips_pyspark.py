# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F

# Read Bronze
bronze = spark.table("bronze.ny_taxi_trips")

silver = (
    bronze
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn("trip_minutes", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("total_amount") > 0)
    .filter(F.col("dropoff_ts") > F.col("pickup_ts"))
    .select(
        "VendorID",
        "pickup_ts",
        "dropoff_ts",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "trip_minutes"
    )
)

# Write Silver as a managed table
(
    silver
    .write
    .mode("overwrite")
    .saveAsTable("silver.trips")
)

display(silver.limit(10))