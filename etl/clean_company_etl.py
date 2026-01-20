import pandas as pd
from etl.unknown_placeholders_replace_etl import replace_unknown_placeholders
from etl.debug_tools_etl import debug_function

def clean_company_parts(s3_parquet_files, bucket, prefix, prefix_2, debug):
    """
    Purpose:
    Build cleaned company outputs from raw parquet parts

    Produces TWO outputs:
    1) A company a dimension-like dataset (one column: company), standardised, with unknowns filled.
    2) A staging/link dataset containing job_link + company (useful for joining back to job postings).

    Notes:
    - Function writes one parquet output per input file to keep memory safe
    - 'unknown_company' is used as a canonical placeholder so downstream tables don't contain messy
      values like '', 'n/a', '-', etc.

    :param s3_parquet_files: List of s3 URIs (s3://...) that point to raw parquet files
    :param bucket: S3 bucket name
    :param prefix: prefix for s3 file for company table
    :param prefix_2: prefix for s3 file for company_w_unknown table
    :param debug: If True, logs diagnostics, if False, does nothing
    """
    for n,file in enumerate(s3_parquet_files):
        df = pd.read_parquet(file, columns=['company','job_link'])


        df['company'] = df['company'].astype('string').str.casefold().str.strip()

        df_staging = pd.DataFrame({'company':df['company'],
                                   'job_link':df['job_link']})
        #Convert messy unknown placeholders to NULLs) so null-handling is consistent.
        df['company'] = replace_unknown_placeholders(df['company'])

        #For the staging/link table do not keep NULL, otherwise a stable join value is lost
        mask_condition = (df['company'].isna())
        df['company'] = df['company'].mask(mask_condition, 'unknown_company')

        #df_staging["company"] contains the raw value
        #df_staging["company_plus_unknown"] is guaranteed non-null for linking/joins
        df_staging['company_plus_unknown'] = df['company']

        df = df[['company']]

        #Optional diagnostics for development/troubleshooting.
        if debug:
            debug_function(df, debug=True, columns=['company'])
            debug_function(df_staging, debug=True, columns=['company','company_plus_unknown','job_link',])

        df.to_parquet(f's3://{bucket}/{prefix}/part_{n:05d}.parquet',index=False)

        df_staging.to_parquet(f's3://{bucket}/{prefix_2}/part_{n:05d}.parquet',index=False)
        
        

        

