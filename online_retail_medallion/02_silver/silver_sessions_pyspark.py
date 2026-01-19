# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---- CONFIG ----
SOURCE_TRIPS_TABLE = "bronze.ny_taxi_trips"   # <-- change if needed
TARGET_SESSIONS_TABLE = "silver.sessions"

# Session rule: new session if gap > 30 minutes
SESSION_GAP_MINUTES = 30

# ---- LOAD ----
trips = spark.table(SOURCE_TRIPS_TABLE)

# Expect these columns exist (common taxi schema)
# VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, passenger_count, trip_distance, total_amount
# If yours use different names, tell me and I’ll adapt quickly.

# ---- CLEAN / STANDARDISE (minimal) ----
t = (
    trips
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn("vendor_id", F.col("VendorID").cast("string"))
    .withColumn("pu_location_id", F.col("PULocationID").cast("int"))
    .withColumn("do_location_id", F.col("DOLocationID").cast("int"))
    .withColumn("trip_distance", F.col("trip_distance").cast("double"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
    .withColumn("passenger_count", F.col("passenger_count").cast("double"))
    .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
    .filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
)

# ---- SESSIONISE ----
# Partition by vendor_id. If you had a driver_id, you'd use that instead.
w = Window.partitionBy("vendor_id").orderBy("pickup_ts")

t2 = (
    t
    .withColumn("prev_dropoff_ts", F.lag("dropoff_ts").over(w))
    .withColumn(
        "gap_minutes",
        (F.col("pickup_ts").cast("long") - F.col("prev_dropoff_ts").cast("long")) / 60.0
    )
    .withColumn(
        "is_new_session",
        F.when(F.col("prev_dropoff_ts").isNull(), F.lit(1))
         .when(F.col("gap_minutes") > F.lit(SESSION_GAP_MINUTES), F.lit(1))
         .otherwise(F.lit(0))
    )
    .withColumn("session_index", F.sum("is_new_session").over(w))
)

# Stable-ish session_id
sessions = (
    t2
    .withColumn(
        "session_id",
        F.concat_ws("-", F.lit("v"), F.col("vendor_id"), F.lit("s"), F.col("session_index").cast("string"))
    )
    .select(
        "session_id",
        "vendor_id",
        "session_index",
        "pickup_ts",
        "dropoff_ts",
        "gap_minutes",
        "pu_location_id",
        "do_location_id",
        "passenger_count",
        "trip_distance",
        "total_amount",
        # keep originals if you want lineage/debug:
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "VendorID",
        "PULocationID",
        "DOLocationID",
    )
)

# ---- WRITE ----
# If you're using Delta in CE (you likely are), this is fine:
(
    sessions
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(TARGET_SESSIONS_TABLE)
)

print(f"✅ Wrote {TARGET_SESSIONS_TABLE}")
display(spark.table(TARGET_SESSIONS_TABLE).limit(20))