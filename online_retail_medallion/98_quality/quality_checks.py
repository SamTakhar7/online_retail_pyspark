# Databricks notebook source
# MAGIC %sql
# MAGIC -- Fail-fast checks (these should return 0 rows)
# MAGIC -- If they return rows, your pipeline is producing bad data.
# MAGIC
# MAGIC -- 1) No null pickup/dropoff
# MAGIC SELECT *
# MAGIC FROM silver.trips
# MAGIC WHERE pickup_ts IS NULL OR dropoff_ts IS NULL
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- 2) No negative/zero distances
# MAGIC SELECT *
# MAGIC FROM silver.trips
# MAGIC WHERE trip_distance <= 0
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- 3) No negative totals
# MAGIC SELECT *
# MAGIC FROM silver.trips
# MAGIC WHERE total_amount <= 0
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- 4) Dropoff after pickup
# MAGIC SELECT *
# MAGIC FROM silver.trips
# MAGIC WHERE dropoff_ts <= pickup_ts
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.data_quality_violations (
# MAGIC   check_name STRING,
# MAGIC   violation_count BIGINT,
# MAGIC   checked_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO silver.data_quality_violations
# MAGIC SELECT 'null_timestamps' AS check_name, COUNT(*) AS violation_count, current_timestamp()
# MAGIC FROM silver.trips
# MAGIC WHERE pickup_ts IS NULL OR dropoff_ts IS NULL;
# MAGIC
# MAGIC INSERT INTO silver.data_quality_violations
# MAGIC SELECT 'non_positive_distance', COUNT(*), current_timestamp()
# MAGIC FROM silver.trips
# MAGIC WHERE trip_distance <= 0;
# MAGIC
# MAGIC INSERT INTO silver.data_quality_violations
# MAGIC SELECT 'non_positive_total', COUNT(*), current_timestamp()
# MAGIC FROM silver.trips
# MAGIC WHERE total_amount <= 0;
# MAGIC
# MAGIC INSERT INTO silver.data_quality_violations
# MAGIC SELECT 'dropoff_before_pickup', COUNT(*), current_timestamp()
# MAGIC FROM silver.trips
# MAGIC WHERE dropoff_ts <= pickup_ts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.data_quality_violations
# MAGIC ORDER BY checked_at DESC;