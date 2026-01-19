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

## Tables Produced

The pipeline produces a set of cleaned and curated datasets stored as partitioned Parquet files in Amazon S3 and exposed as external tables in Athena.

Tables Produced

The pipeline produces a set of cleaned and curated datasets stored as partitioned Parquet files in Amazon S3 and exposed as external tables in Athena.

**Raw Layer**

raw/job_postings
Raw ingestion of CSV data converted to Parquet and partitioned by run_date.
Acts as the base landing layer for downstream transforms.

**Cleaned Layer**

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

**Curated Layer**

curated/job_postings
Deduplicated job postings using natural keys, validated and analytics-ready.

curated/job_postings_skills_stage
Deduplicated job–skill relationships suitable for skill-based analysis.

curated/skills
Canonical list of unique skills extracted from job postings.

curated/job_title
Canonical set of job titles derived from cleaned titles.

curated/job_type
Canonical list of job types.

curated/location
Canonical list of job locations.

curated/job_level
Canonical job seniority levels.

curated/company / curated/company_w_unknown
Canonical company dimension and a variant preserving the company name set on the job posting.

Notes

All tables are partitioned by run_date unless otherwise noted.

Curated tables are rebuilt per partition to support safe idempotent reruns.

Athena partitions are added after successful Parquet writes to make data immediately queryable.


---

## How to Run

**Prerequisties**
- Python 3.9+
- Apache Airflow
- AWS account with access to S3 and Athena
- Local copy of the LinkedIn job postings CSV dataset

### Environment Variables
The pipeline uses environment variables for environment-specific configuration.  
See `.env.example` for a complete list.

Required variables include:
- `BUCKET` – S3 bucket used for the data lake
- `RAW_ROOT_PREFIX` – S3 prefix for raw datasets
- `CLEAN_ROOT_PREFIX` – S3 prefix for cleaned datasets
- `CURATED_ROOT_PREFIX` – S3 prefix for curated datasets
- `AWS_REGION` – AWS region for Athena queries
- `ATHENA_WORKGROUP` – Athena workgroup name
- `ATHENA_RESULTS_S3` – S3 location for Athena query results

### Running the Pipeline
1. Start the Airflow scheduler and webserver.
2. Enable the `linkedin_etl_pipeline` DAG.
3. Trigger the DAG manually for a chosen execution date.
4. After completion, curated datasets will be available in S3 and queryable via Athena.

---


## Known Limitations & Assumptions

- **Batch pipeline (not real-time):** The workflow is triggered manually in Airflow and processes data in batch using the execution date (_run_date_).

- **Raw layer is file-based ingestion:** Source CSVs are assumed to exist on the local filesystem (e.g. a Kaggle download). This project does not include automated dataset retrieval from Kaggle/API.

- **Idempotency scope:** Cleaned/curated datasets are designed to be idempotent per _run_date_ partition (reruns rebuild that partition). The raw layer is treated as a landing zone and may contain multiple outputs if the same _run_date_ is re-ingested.

- **Natural keys instead of surrogate IDs:** Tables rely on natural keys (e.g. _job_link_) for uniqueness rather than warehouse-style surrogate keys. This is sufficient for Athena-based analytics but is not a full dimensional warehouse design.

- **In-memory global deduplication:** Cross-file deduplication uses in-memory Python sets for simplicity. This is appropriate for this project’s scale, but very large cardinalities would require a distributed approach (e.g. Spark) or a database-backed strategy.

- **Canonical “unknown” categories:** Placeholder values are normalised to NULL and then mapped to canonical labels (e.g. _unknown_company_, _unknown_location_) where stable join keys are required for analytics.

- **Athena partitions must match S3 layout:** Athena tables assume the S3 prefixes follow the _.../run_date=YYYY-MM-DD/_ convention. If prefixes change, the Data Definition Language(DDL)/partition SQL must be updated accordingly.
