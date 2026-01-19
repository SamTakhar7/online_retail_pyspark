# 🚕 NYC Taxi Analytics — Databricks Medallion Architecture (SQL + PySpark)

## Overview
This project demonstrates an end-to-end analytics pipeline built in **Databricks Community Edition**, using a **Medallion Architecture (Bronze → Silver → Gold)** to transform raw NYC Taxi trip data into analytics-ready datasets.

The focus is not just on transformations, but on **design choices**:
- When SQL is sufficient
- When PySpark is more appropriate
- How this would scale in a production Azure + Databricks environment

The project mirrors how a real analytics team would structure work in Databricks when dbt is not the primary orchestration layer.

---

## Dataset
**NYC Yellow Taxi Trips – January 2023**

Source: NYC Taxi & Limousine Commission (TLC)  
Format: Parquet  
Volume: Millions of trip records

---

## Architecture (Conceptual)

Raw (ingested data)
↓
Bronze (light standardisation, minimal assumptions)
↓
Silver (business logic, sessionisation, enrichment)
↓
Gold (analytics-ready metrics for decision-making)

This mirrors how Databricks is typically used in Azure environments:
- SQL for declarative transformations
- PySpark where stateful or non-trivial logic is required
- Tables stored as Delta tables for reliability and performance

---

## Project Structure
├── bronze/
│   └── bronze_trips.sql
├── silver/
│   ├── silver_trips.sql
│   └── silver_sessions.sql
├── gold/
│   └── gold_trip_metrics.sql
├── pyspark/
│   └── build_sessions.py
├── run_all.sql
└── README.md

---

## Layers Explained

### 🟫 Bronze Layer
**Purpose:**  
Preserve source fidelity while enforcing basic structure.

**Key characteristics:**
- Minimal transformations
- Column renaming and type casting
- No business assumptions

**Example logic:**
- Rename fields to snake_case
- Cast timestamps and numeric fields
- Filter obviously invalid records (e.g. null timestamps)

---

### 🟪 Silver Layer
**Purpose:**  
Apply business logic and create reusable analytical building blocks.

**Key characteristics:**
- Enriched datasets
- Reusable entities (trips, sessions)
- Clear business meaning

**What happens here:**
- Trip-level cleansing
- Sessionisation logic
- Feature derivation (durations, flags, aggregates)

---

### 🟨 Gold Layer
**Purpose:**  
Deliver decision-ready datasets for analytics, BI, and downstream products.

**Key characteristics:**
- Aggregated metrics
- Stable interfaces for reporting
- Minimal joins required downstream

**Example outputs:**
- Average trip duration
- Revenue metrics
- Customer or session-level KPIs

---

## Why PySpark (and not only SQL)?
SQL is excellent for **set-based, declarative transformations**.  
However, PySpark is more appropriate when:
- Stateful logic is required (e.g. sessionisation)
- Complex window logic becomes unreadable in SQL
- Performance tuning is needed at scale
- Logic needs to be reused programmatically

In this project:
- **SQL** is used for Bronze and most Silver transformations
- **PySpark** is used to construct session-level tables where SQL would be brittle or opaque

This mirrors real Databricks usage in production environments.

---

## Orchestration Strategy
In Databricks CE, orchestration is demonstrated via a `run_all.sql` script that executes layers in order.

In a production Azure setup, this would typically be replaced with:
- **Databricks Jobs** (for execution)
- **Azure Data Factory** (for scheduling and upstream ingestion)
- Optional dbt Core for SQL-only transformation layers

---

## Data Quality & Governance
While Databricks CE does not include full production tooling, this project is designed with governance in mind:
- Clear layer boundaries
- Deterministic transformations
- Stable table contracts at the Gold layer
- Logic structured for easy testing and extension

In production, this would be extended with:
- Expectations / checks (e.g. Delta Live Tables, Great Expectations)
- Metadata cataloguing
- Access controls via Unity Catalog

---

## How This Scales in Azure
In an enterprise Azure environment, this design would map cleanly to:
- **ADF** → ingest raw data on schedule
- **Databricks Jobs** → execute Bronze/Silver/Gold pipelines
- **Delta Lake** → reliable storage with ACID guarantees
- **Power BI / downstream tools** → consume Gold tables

The architecture prioritises **clarity, ownership, and trust**, not just technical execution.

---

## Why This Project Exists
This project was built to demonstrate:
- Practical Databricks usage beyond toy examples
- When to choose SQL vs PySpark
- How analytics engineering principles apply outside Snowflake/dbt-only stacks
- Readiness for Principal / Lead analytics roles in Azure + Databricks environments

---

## Author
**Sam Takhar**  
Analytics Engineer / Principal Analyst  
Specialising in data modelling, analytics platforms, and business-aligned data systems
