# 🚕 Transportation Data Pipeline (Databricks Lakeflow)

A end-to-end **Medallion Architecture** data pipeline built on **Databricks Lakeflow (Spark Declarative Pipelines)** that ingests, cleans, and models ride-sharing/transportation data for 10 major Indian cities. The pipeline streams raw trip data from S3, applies data quality checks and Change Data Capture (CDC), and produces a star-schema analytics layer with city-level views.

---

## Overview

| | |
|---|---|
| **Platform** | Databricks Lakehouse (Serverless compute, Photon engine) |
| **Framework** | Lakeflow Spark Declarative Pipelines (Python + SQL) |
| **Storage** | Delta Lake on AWS S3 (`s3://goodcabs-harrymugi32/data-store/`) |
| **Catalog** | Unity Catalog — `transportation` |
| **Pipeline mode** | Triggered (on-demand execution) |
| **Data volume** | ~366K trip records across 10 cities, Aug–Dec 2025 |

---

## Architecture

The pipeline follows the classic Bronze → Silver → Gold pattern, with each layer serving a distinct purpose:

```
S3 Source Data
   │
   ├─ city.csv ───────────▶ bronze.city ──────▶ silver.city ─────┐
   │                        (batch MV)          (cleaned)         │
   │                                                               │
   └─ trips/*.csv ────────▶ bronze.trips ─────▶ silver.trips ─────┼──▶ gold.fact_trips
        (Auto Loader)       (streaming table)   (CDC + DQ)        │      (star schema)
                                                                   │           │
        generated ─────────────────────────────▶ silver.calendar ─┘           │
        (date sequence)                          (dimension MV)               │
                                                                               ├─▶ fact_trips_jaipur
                                                                               ├─▶ fact_trips_lucknow
                                                                               ├─▶ fact_trips_mysore
                                                                               ├─▶ ... (7 more cities)
                                                                               └─▶ fact_trips_chandigarh
```

| Layer | Purpose |
|---|---|
| **Bronze** | Raw ingestion from S3 with minimal transformation. Adds source metadata (`file_name`, `ingest_datetime`). Trips are ingested continuously via **Auto Loader**; city data is a batch load. |
| **Silver** | Cleansing, standardization, and enrichment. Trips are deduplicated and upserted via **Auto CDC (SCD Type 1)**, with data quality expectations enforced on load. A generated calendar dimension is also built here. |
| **Gold** | Business-ready star schema. A central fact table joins trips, city, and calendar dimensions, with 10 city-scoped views for localized reporting. |

---

## Project Structure

```
transportation-databricks-project/
├── transformations/
│   ├── bronze/
│   │   ├── city.py          # City dimension batch ingestion
│   │   └── trips.py         # Trips streaming ingestion (Auto Loader)
│   ├── silver/
│   │   ├── city.py          # City dimension cleaning
│   │   ├── calendar.py      # Generated date dimension
│   │   └── trips.py         # Trips CDC + data quality expectations
│   └── gold/
│       ├── trips_gold.sql        # Main fact table (fact_trips)
│       ├── trips_jaipur.sql      # City-specific views (×10)
│       ├── trips_lucknow.sql
│       ├── trips_mysore.sql
│       ├── trips_visakhapatnam.sql
│       ├── trips_surat.sql
│       ├── trips_vadodara.sql
│       ├── trips_indore.sql
│       ├── trips_coimbatore.sql
│       ├── trips_kochi.sql
│       └── trips_chandigarh.sql
├── .gitignore
└── README.md
```

---

## Data Model

### Bronze layer

| Table | Type | Description |
|---|---|---|
| `bronze.city` | Materialized view | 10 cities loaded from CSV via batch ingestion, with corrupt-record handling |
| `bronze.trips` | Streaming table | Trip events ingested via Auto Loader with schema inference, evolution, and a `_rescued_data` column for schema mismatches |

### Silver layer

| Table | Type | Description |
|---|---|---|
| `silver.city` | Materialized view | Standardized city dimension |
| `silver.trips` | Streaming table (CDC) | Trip facts upserted with **SCD Type 1** logic, keyed on trip ID and sequenced by `silver_processed_timestamp`. Enforces data quality expectations: valid trip date (year ≥ 2020), and passenger/driver ratings in range 1–10 |
| `silver.calendar` | Materialized view | Generated date dimension (year, quarter, month, week, weekday/weekend flags, Indian national holidays) driven by `start_date` / `end_date` pipeline parameters |

### Gold layer

| Object | Description |
|---|---|
| `gold.fact_trips` | Star-schema fact table joining `silver.trips`, `silver.city`, and `silver.calendar` |
| `fact_trips_<city>` (×10) | Per-city views filtered from `fact_trips` — Jaipur, Lucknow, Mysore, Visakhapatnam, Surat, Vadodara, Indore, Coimbatore, Kochi, Chandigarh |

---

## Key Engineering Patterns

- **Auto Loader** for incremental, cloud-native streaming ingestion of trip files from S3, with automatic schema inference/evolution and configurable throughput (100 files/trigger).
- **Auto CDC (SCD Type 1)** for the silver trips table — a staging view feeds a CDC flow that automatically merges/upserts records, removing the need for hand-written `MERGE INTO` logic.
- **Declarative data quality expectations** (`@dp.expect`) on the silver layer to validate business rules (valid dates, rating ranges) without failing the pipeline — violations are tracked in lineage instead.
- **Delta Lake optimizations** across all layers: Change Data Feed, auto-optimize writes, and auto-compaction for small-file consolidation.
- **Parameterized pipeline** — `start_date` / `end_date` control the generated calendar dimension's range without code changes.
- **Star-schema modeling** in the gold layer, with per-city views built on top of a single fact table to simplify regional reporting.

---

## Pipeline Configuration

```yaml
name: transportation_pipeline
mode: triggered
compute: serverless (Photon-enabled)
catalog: transportation

parameters:
  start_date: 2025-01-01
  end_date: 2025-12-31

sources:
  city:  s3://goodcabs-harrymugi32/data-store/city
  trips: s3://goodcabs-harrymugi32/data-store/trips
```

---

## Getting Started

### Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Serverless compute enabled
- Read access to the S3 source bucket

### Running the pipeline

1. Clone this repository into a Databricks Git folder (or attach it as a Lakeflow pipeline source).
2. Configure the `transportation_pipeline` with the `start_date` / `end_date` parameters.
3. Run a **dry run** to validate the DAG.
4. Trigger a full pipeline update to materialize bronze → silver → gold.

### Adding a new dataset

1. Add a new `.py` (or `.sql`) file under the relevant layer (`transformations/bronze/`, `silver/`, or `gold/`).
2. Name the file after the dataset it produces (e.g. `drivers.py`).
3. Implement the transformation using the Spark Declarative Pipelines API.
4. Validate with a dry run before triggering a production update.

---

## Git Workflow

This project follows a feature-branch workflow:

- `main` — production-ready pipeline code
- `Feature/SilverProcessing` — bronze + silver layer development
- `Feature/GoldProcessing` — gold layer analytics (fact table + city views)

Changes are validated with a pipeline dry run before being merged into `main`.

---

## Possible Next Steps

- Driver dimension table and payment transaction data
- Peak-hour / demand-forecasting analytics
- Alerting on data quality expectation violations
- Automated CI checks for pipeline dry runs on PRs

---

## Tech Stack

`Python` · `SQL` · `PySpark` · `Databricks Lakeflow (Spark Declarative Pipelines)` · `Delta Lake` · `Unity Catalog` · `AWS S3`
