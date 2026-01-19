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

            +----------------------+
            |   Kaggle CSV files   |
            |   (local download)   |
            +----------+-----------+
                       |
                       v
        +-----------------------------------+
        | RAW LAYER (S3, Parquet)           |
        | s3://.../raw/run_date=YYYY-MM-DD/ |
        +------------------+----------------+
                           |
                           v
        +--------------------------------------+
        | CLEANED LAYER (S3, Parquet)           |
        | - text normalisation                  |
        | - placeholder -> NULL standardisation |
        | s3://.../cleaned/<table>/run_date=... |
        +------------------+-------------------+
                           |
                           v
        +--------------------------------------+
        | CURATED LAYER (S3, Parquet)           |
        | - dedup using natural keys            |
        | - validation checks                   |
        | - idempotent reruns (partition rebuild)|
        | s3://.../curated/<table>/run_date=... |
        +------------------+-------------------+
                           |
                           v
        +--------------------------------------+
        | ATHENA (SQL)                          |
        | - CREATE EXTERNAL TABLE               |
        | - ALTER TABLE ADD PARTITION           |
        +--------------------------------------+

Orchestrated by: Apache Airflow
