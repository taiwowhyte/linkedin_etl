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
