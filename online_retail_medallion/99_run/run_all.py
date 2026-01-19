# Databricks notebook source
# DBTITLE 1,Untitled
# Setup
dbutils.notebook.run('../00_setup/create_schemas', 60)

# Bronze
dbutils.notebook.run('../01_bronze/bronze_ingestion', 60)

# Silver
dbutils.notebook.run('../02_silver/silver_trips_pyspark', 60)
dbutils.notebook.run('../02_silver/silver_sessions_pyspark', 60)


dbutils.notebook.run('../98_quality/quality_checks', 60)

# Gold
dbutils.notebook.run('../03_gold/gold_trip_metrics', 60)
dbutils.notebook.run('../03_gold/session_metrics', 60)