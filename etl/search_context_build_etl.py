import pandas as pd
from etl.debug_tools_etl import debug_function
from etl.parquet_validation_etl import validate_table
from etl.unknown_placeholders_replace_etl import replace_unknown_placeholders

def cleaning_search_context_table(s3_parquet_files, wanted_columns, bucket, prefix, debug=False):
    """
    Build the cleaned search_context dataset from raw parquet parts.

    Purpose:
    - Extract and standardise search context fields

    Cleaning steps:
    - Select only wanted columns (schema enforcement)
    - Normalise whitespace (collapse multiple spaces), strip, and casefold for stable values
    - Convert common 'unknown' placeholders to NULLS
    - Deduplicate rows (this table is treated as a unique set of search context fields)

    Output:
    - Writes one parquet file per input part to prevent OOM problems
    """
    n = 1
    for file in s3_parquet_files:
        df = pd.read_parquet(file)

        df = df[wanted_columns].astype('string').apply(lambda col:col.str.replace(r'\s+', ' ', regex=True))

        df = df.apply(lambda col:col.str.strip().str.casefold())

        df = replace_unknown_placeholders(df)

        df = df.drop_duplicates(ignore_index=True)

        if debug:
            debug_function(df,False, wanted_columns)

        df.to_parquet(f's3://{bucket}/{prefix}/part_{n:05d}.parquet',index=False)

        n+=1

