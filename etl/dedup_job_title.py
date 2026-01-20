import pandas as pd
from etl.parquet_validation_etl import validate_table
from etl.delete_s3_URI import delete_s3_prefix

def curate_job_titles_streaming(s3_parquet_files,bucket,curated_prefix,var_char,run_date):
    #Curated output is partitioned by run_date for idempotent reruns
    out_prefix = f'{curated_prefix}/run_date={run_date}'

    #Make an array of unique canonical_job_title values across all input parts
    seen = set()

    for file in s3_parquet_files:
        df = pd.read_parquet(file, columns=['canonical_job_title'])
        vals = df['canonical_job_title'].dropna().astype('string').unique().tolist()
        for v in vals:
            seen.add(v)

    canonical_df = pd.DataFrame({'canonical_job_title': list(seen)})
    
    # Validate uniqueness + varchar limits before deleting/writing
    validate_table(canonical_df,wanted_columns=['canonical_job_title'],VAR_CHAR_LIMITS=var_char,natural_key_columns=['canonical_job_title'])

    #Idempotency: delete the run_date partition once there is at least one bit of valid data to write
    delete_s3_prefix(bucket, out_prefix)

    
    out_path = f's3://{bucket}/{out_prefix}/data.parquet'
    canonical_df.to_parquet(out_path, index=False)
