import pandas as pd
from etl.env import LINKEDIN_DATA_DIR,BUCKET,RAW_ROOT_PREFIX
import os

# ** means this function can accept any number of named arguments, and I'll collect them into a dict
def convert_csv_to_parquet(**run_info):
    """
    Ingest raw CSV exports and write partitioned Parquet to S3.
    Airflow passes context kwargs. Use run_info['ds'] as the run_date partition key. This results in reproducibility and idempotent reruns at partition level).
    """
    run_date = run_info['ds']

    base_dir = LINKEDIN_DATA_DIR

    job_summary_path = os.path.join(base_dir, 'job_summary.csv')
    job_skills_path = os.path.join(base_dir, 'job_skills.csv')
    job_postings_path = os.path.join(base_dir, 'linkedin_job_postings.csv')

    with open(job_summary_path) as f1:
        job_summary = pd.read_csv(f1)

    with open(job_skills_path) as f2:
        job_skills = pd.read_csv(f2)


    summary_df = pd.DataFrame(job_summary)
    skills_df = pd.DataFrame(job_skills)

    #normalise job_link first so can then merge
    df_list = [summary_df, skills_df]

    for item in df_list:
        item['job_link'] = item['job_link'].astype('string')
        #logging.info('data type values in column job_link: %s', item['job_link'].astype('string'))
        item['job_link'] = item['job_link'].str.strip()

    #process job_postings file in chunks as it is extremely large
    with open(job_postings_path) as f:
        for i, chunk in enumerate(pd.read_csv(f, chunksize=10000)):
            chunk['job_link'] = chunk['job_link'].astype('string').str.strip()
            df = chunk.merge(summary_df, how='left', on='job_link')
            df_1 = df.merge(skills_df, how='left', on='job_link')

            #write to s3
            #adding run date causes idempotency, partitioning, reproducibility
            
            out_path = f's3://{BUCKET}/{RAW_ROOT_PREFIX}/run_date={run_date}/part_{i+1}.parquet'
            df_1.to_parquet(out_path, index=False)
