import pandas as pd
from etl.parquet_validation_etl import validate_table
from etl.delete_s3_URI import delete_s3_prefix


def curated_job_posting(s3_parquet_files, natural_key_columns,bucket,prefix,wanted_columns,VAR_CHAR_LIMITS,run_date):
    #This function approach assumes a single natural key: job_link
    if natural_key_columns != ['job_link'] and natural_key_columns != ('job_link'):
        raise ValueError(
            f"This streaming implementation currently expects natural_key_columns=['job_link'], "
            f'got {natural_key_columns}'
        )

    #Cross-file dedup to ensure one row per job_link across all input parts
    seen_links = set()

    #Partitioned output location for idempotent reruns of the same run_date
    out_base = f's3://{bucket}/{prefix}/run_date={run_date}/'
    out_prefix_key = f'{prefix}/run_date={run_date}/'
    
    #delete output partition only once, and only after we have valid data
    deleted = False
    part = 1

    for file in s3_parquet_files:
        df = pd.read_parquet(file)

        if wanted_columns:
            df = df[wanted_columns]

        df = df.drop_duplicates(subset=['job_link'], keep='first')

        #Cross-file dedup: drop any job_links already written in earlier parts
        mask_new = ~df['job_link'].isin(seen_links)
        df = df.loc[mask_new]

        new_links = df['job_link'].dropna().astype('string').tolist()
        seen_links.update(new_links)

        #Skip empty chunks (nothing new to write)
        if df.empty:
            continue

        #Validate before deleting existing output (prevents wiping good data if this run is bad)
        validate_table(df, wanted_columns, VAR_CHAR_LIMITS, natural_key_columns)

        #Idempotency: delete the run_date partition once it's known that there is least one valid chunk to write
        if not deleted:
            delete_s3_prefix(bucket, out_prefix_key)
            deleted = True


        df.to_parquet(f'{out_base}part_{part:04d}.parquet', index=False)
        part += 1
