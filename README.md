# linkedin_etl Pipeline

**Overview**

This project implements an end-to-end data engineering ETL pipeline for LinkedIn job postings data using Python, Pandas, AWS S3, Athena, and Airflow. The pipeline ingests raw CSV data, transforms it into cleaned and curated Parquet datasets partitioned by run date, and exposes the final tables via Athena for dashboard analytics and querying.

The goal of this project was to gain hands-on experience designing a complete, production style ETL, with a focus on streaming and chunked processing for large datasets, safe idempotent reruns for curated datsets, orchestrated execution with Airflow and cloud-based data processing using Amazon S3 and Amazon Athena. 

---

## Tech Stack

**Languages & Libraries**
- Python
- SQL (Athena)

**Core Libraries**
- Pandas
- boto3

**Orchestration**
- Apache Airflow

**Cloud & Storage**
- Amazon S3 (Parquet-based data lake)

**Query & Analytics**
- Amazon Athena

**Data Formats**
- CSV (raw ingestion)
- Parquet (cleaned and curated layers)


---

## Pipeline Architecture

Raw CSV files (Kaggle download, local filesystem)
        ↓
S3 Raw Layer (Parquet, run_date partitioned)
        ↓
S3 Cleaned Layer
  - text normalisation
  - placeholder → NULL standardisation
        ↓
S3 Curated Layer
  - natural key deduplication
  - validation (schema, nulls, varchar limits)
  - idempotent reruns per run_date
        ↓
Athena External Tables
  - CREATE EXTERNAL TABLE
  - ADD PARTITION (run_date)

Orchestrated by: Apache Airflow


Key design notes:

The pipeline follows a layered data lake design (raw → cleaned → curated).

All datasets are partitioned by run_date to support reproducibility and efficient querying.

Curated outputs are rebuilt per partition to allow safe idempotent reruns.

Airflow orchestrates execution and ensures transforms complete before Athena partitions are added.


---

Tables Produced

The pipeline produces a set of cleaned and curated datasets stored as partitioned Parquet files in Amazon S3 and exposed as external tables in Athena.

Raw Layer

raw/job_postings
Raw ingestion of CSV data converted to Parquet and partitioned by run_date.
Acts as the immutable landing layer for downstream transforms.

Cleaned Layer

cleaned/job_postings_staging
Normalised job posting records with standardised text fields and basic cleaning applied.

cleaned/job_postings_skills_staging
Exploded job skills (one skill per row per job) for easier downstream deduplication.

cleaned/search_context
Standardised text fields used for grouping and search-based analytics.

cleaned/job_title_staging
Cleaned job titles with canonicalisation logic applied.

cleaned/location
Standardised job locations with consistent handling of unknown values.

cleaned/company / cleaned/company_w_unknown
Cleaned company names, including a variant that preserves canonical unknown placeholders for linking.

Curated Layer

curated/job_postings
Deduplicated job postings using natural keys, validated and analytics-ready.

curated/job_postings_skills_stage
Deduplicated job–skill relationships suitable for skill-based analysis.

curated/skills
Canonical list of unique skills extracted from job postings.

curated/job_title
Canonical set of job titles derived from cleaned titles.

curated/job_type
Canonical job types (e.g. full-time, part-time).

curated/location
Canonical list of job locations.

curated/job_level
Canonical job seniority levels.

curated/company / curated/company_w_unknown
Canonical company dimension and a variant preserving job-level linking.


