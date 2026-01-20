import pandas as pd
from etl.parquet_validation_etl import validate_table
from etl.delete_s3_URI import delete_s3_prefix

def curate_job_postings_skills_stage_streaming(s3_parquet_files, natural_key_columns,bucket,curated_prefix,wanted_columns,VAR_CHAR_LIMITS,run_date):
    #check that have the expected natural key combo
    expected = ['job_skills', 'job_link']
    if list(natural_key_columns) != expected:
        raise ValueError(f'Expected natural_key_columns={expected}, got {natural_key_columns}')

    out_base = f's3://{bucket}/{curated_prefix}/run_date={run_date}/'

    out_prefix_key = f'{curated_prefix}/run_date={run_date}/'

    #cross-file dedup so each (job_skills, job_link) pair appears once
    part = 1
    seen_keys = set()
    part = 1
    deleted=False

    for file in s3_parquet_files:
        df = pd.read_parquet(file)

        df = df[wanted_columns]
        df = df.drop_duplicates(subset=expected, keep='first')

        #Build tuple keys as strings for consistent comparisons across files
        key_df = df[expected].astype('string')
        keys = list(zip(key_df['job_skills'], key_df['job_link']))

        #Streaming global dedup across files
        keep_mask = [k not in seen_keys for k in keys]
        df = df.loc[keep_mask]

        #Update seen set
        seen_keys.update([k for k, keep in zip(keys, keep_mask) if keep])

        if df.empty:
            continue

        validate_table(df, wanted_columns, VAR_CHAR_LIMITS, natural_key_columns)

        if not deleted:
            delete_s3_prefix(bucket, out_prefix_key)
            deleted=True

        df.to_parquet(f'{out_base}part_{part:04d}.parquet', index=False)
        part += 1
