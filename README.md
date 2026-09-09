# Transportation Data Pipeline

A Lakeflow Spark Declarative Pipeline (SDP) for processing transportation data using the Medallion Architecture (Bronze → Silver → Gold) on Databricks.

## Architecture

The pipeline follows a three-layer medallion architecture:

| Layer | Purpose | Description |
| --- | --- | --- |
| Bronze | Raw ingestion | Loads raw data from S3 sources with minimal transformation, adds metadata columns (file source, ingestion timestamp) |
| Silver | Cleansing & enrichment | Standardizes data types, deduplicates, applies data quality rules, and enriches with reference data |
| Gold | Aggregation & analytics | Business-level aggregations, KPIs, and curated datasets ready for reporting and dashboards |

## Project Structure

```
transportation-databricks-project/
├── transformations/
│   ├── bronze/          # Raw data ingestion layer
│   │   └── city.py      # City data ingestion from S3 CSV source
│   ├── silver/          # Data cleansing and standardization
│   └── gold/            # Business aggregations and analytics
├── README.md
└── .gitignore
```

## Data Source

Raw transportation data is ingested from S3:

* **City data**: `s3://goodcabs-harrymugi32/data-store/city` (CSV format)

## Pipeline Configuration

| Setting | Value |
| --- | --- |
| Catalog | `transportation` |
| Schema | `bronze` |
| Compute | Serverless |
| Runtime | Photon-enabled |
| Channel | Current |

## Unity Catalog Tables

Pipeline datasets are published to Unity Catalog under the `transportation` catalog:

* `transportation.bronze.city` — Raw city data with ingestion metadata

## Development

### Adding a New Dataset

1. Create a new `.py` file under the appropriate layer folder (`transformations/bronze/`, `transformations/silver/`, or `transformations/gold/`)
2. Name the file after the dataset (e.g., `trips.py` for a trips dataset)
3. Implement the transformation using Spark Declarative Pipelines APIs
4. Run a dry run to validate, then trigger a pipeline update

### Prerequisites

* Databricks workspace with Unity Catalog enabled
* Serverless compute enabled
* S3 access to the source data bucket

## Git Workflow

1. Make changes on a feature branch
2. Validate with a dry run
3. Commit and push changes
4. Create a pull request for review

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-dataset`)
3. Commit your changes with a descriptive message
4. Push to your fork and submit a pull request