import pandas as pd
from etl.parquet_validation_etl import validate_table
from etl.delete_s3_URI import delete_s3_prefix


def curate_location_streaming(s3_parquet_files,natural_key_columns,bucket,curated_prefix,wanted_columns,VAR_CHAR_LIMITS):
    out_base = f's3://{bucket}/{curated_prefix}/'

    #Cross-file dedup so each location appears once across all input parts
    seen = set()
    n = 1
    deleted=False

    for file in s3_parquet_files:
        #Read only the column needed for this curated dimension-style output as it causes fewer S3 timeouts
        df = pd.read_parquet(file)[['job_location']]

        df = df.drop_duplicates(subset=natural_key_columns, keep='first')

        #Cross-file dedup: keep only locations not already produced
        keys = df['job_location'].astype('string').tolist()
        keep_mask = [k not in seen for k in keys]

        df = df.loc[keep_mask]
        #Update seen with the new locations kept
        seen.update([k for k, keep in zip(keys, keep_mask) if keep])
        
        #Skip if nothing new to write from this part
        if df.empty:
            continue

        validate_table(df, wanted_columns, VAR_CHAR_LIMITS, natural_key_columns)

        #Idempotent reruns: clear existing curated outputs before writing refreshed parts
        if not deleted:
            delete_s3_prefix(bucket, f'{curated_prefix}/')
            deleted=True

        df.to_parquet(f'{out_base}part_{n:04d}.parquet', index=False)
        n+=1
