import pandas as pd
from etl.delete_s3_URI import delete_s3_prefix

def build_job_type(s3_parquet_files, bucket, prefix):
    #Build a dimension-style table of unique job_type values across all parts
    unique_values = set()
    na = 0
    empty = 0

    for file in s3_parquet_files:
        # Column pruning: read only job_level from Parquet to avoid pulling wide rows over the network. It faster & causes fewer S3 timeouts)
        series = (pd.read_parquet(file,columns=['job_type'])['job_type'].astype('string').str.casefold().str.strip())
        
        #Enforce data quality. Curated columns should not contain NULL/empty strings
        na += int(series.isna().sum())
        empty += int((series == '').sum())

        # Keep only real values, then add uniques to the set
        na_removed = series.dropna()
        na_and_empty_strings_removed = na_removed[na_removed != '']
        unique_values.update(na_and_empty_strings_removed.unique())

    if na:
        raise ValueError(f'{na} NAs in job_type')
    if empty:
        raise ValueError(f'{empty} empty strings in job_type')
    
    #Idempotency: delete existing output before writing the new canonical list
    delete_s3_prefix(bucket,f'{prefix}/')

    out_uri = f's3://{bucket}/{prefix}/data.parquet'
    pd.DataFrame({'job_type': sorted(unique_values)}).to_parquet(out_uri, index=False)
