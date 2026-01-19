import pandas as pd
from etl.debug_tools_etl import debug_function
from etl.unknown_placeholders_replace_etl import replace_unknown_placeholders


def cleaning_job_posting_staging(s3_parquet_files, wanted_columns, bucket, prefix, debug=False):
    '''
    Docstring for cleaning_job_posting_staging

    Purpose:
    Convert raw job_postings data to clean/normalised staging data (no deduplication). This data is procressed one parquet part at a time and is written out to S3
    for memory safety. Output files use consistent, order part names under a run_date prefix, making reruns idempotent. Debugging is optional and is disabled as default
    as it memory-intensive.
    
    :param s3_parquet_files: List of s3 URIs (s3://...) that point to raw parquet files
    :param wanted_columns: Columns to keep for schema creation
    :param bucket: S3 bucket name for output
    :param prefix: S3 prefix for output
    :param debug: If True, logs diagnostic summarise. Default False
    :return: None (writes parquet parts to s3)
    '''
    n = 1
    for file in s3_parquet_files:
        df = pd.read_parquet(file)
        df_new = df[wanted_columns].astype('string').apply(lambda col:col.str.replace(r'\s+', ' ', regex=True))
        df_new = df_new.apply(lambda col:col.str.strip().str.casefold())

        df_new = replace_unknown_placeholders(df_new)
        #run the debug finction
        if debug:
            debug_function(df_new,True, wanted_columns)
        #build cleaned dataframe
        
        df_new.to_parquet(f's3://{bucket}/{prefix}/part_{n:05d}.parquet',index=False)
        
        n+=1
