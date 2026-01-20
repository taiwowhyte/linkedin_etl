import pandas as pd
from etl.unknown_placeholders_replace_etl import replace_unknown_placeholders
from etl.debug_tools_etl import debug_function

def clean_location(s3_parquet_files, bucket, prefix, debug=False):
    """
    Purpose:
    Extract and standardise job_location from raw parquet parts, then write cleaned parquet to S3

    Cleaning logic:
    - Normalise casing and whitespace for consistent grouping/dedup downstream
    - Convert common placeholder strings (e.g. '', 'n/a', '-') into NULLs
    - Fill missing locations with a single canonical label ('unknown_location') so downstream joins/metrics
      do not have a mix of placeholder values

    Output:
    - Writes one parquet file per input part to stop OOM problems
    """
    for n,file in enumerate(s3_parquet_files):
        df = pd.read_parquet(file)

        df['job_location'] = df['job_location'].astype('string').str.casefold().str.strip()

        df['job_location'] = replace_unknown_placeholders(df['job_location'])

        mask_condition = (df['job_location'].isna())

        df['job_location'] = df['job_location'].mask(mask_condition, 'unknown_location')

        df = df['job_location']

        if debug:
            debug_function(df, debug=True, columns = 'job_location')
        
        pd.DataFrame({'job_location':df}).to_parquet(f's3://{bucket}/{prefix}/part_{n:05d}.parquet',index=False)
