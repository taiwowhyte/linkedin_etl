import pandas as pd
from etl.delete_s3_URI import delete_s3_prefix
from etl.parquet_validation_etl import validate_table

def curate_company_streaming(s3_parquet_files, unknown_files, bucket, curated_prefix, curated_prefix_w_unknown,VAR_CHAR_LIMITS_company, 
                             VAR_CHAR_LIMITS_company_w_unknown):       

    out_base = f's3://{bucket}/{curated_prefix}'
    
    #Cross-file dedup so each company appears once in curated output
    seen = set()
    n = 1
    wanted_column = ['company']
    deleted=False

    for file in s3_parquet_files:
        # column projection: only read what is needed to minimise s3 timeouts
        df = pd.read_parquet(file, columns=['company'])[['company']]

        # within-file dedup
        df = df.drop_duplicates(subset=['company'], keep='first')

        # global dedup
        keys = df['company'].astype('string').tolist()
        keep_mask = [k not in seen for k in keys]

        df = df.loc[keep_mask]
        seen.update([k for k, keep in zip(keys, keep_mask) if keep])

        if df.empty:
            continue

        validate_table(df, wanted_column, VAR_CHAR_LIMITS_company, natural_key_columns=['company'])

        if not deleted:
            # Idempotent rerun: clear curated company output once, then stream write parts
            delete_s3_prefix(bucket, f"{curated_prefix}")
            deleted=True
        
        df.to_parquet(f'{out_base}/part_{n:04d}.parquet', index=False)
        n += 1



 # CURATE COMPANY, COMPANY_W_UNKNOWN,JOB_LINK table (validate only)

    out_base_unknowns = f's3://{bucket}/{curated_prefix_w_unknown}'
 
    deleted_1=False
    m = 1
    wanted_cols = ['job_link', 'company', 'company_plus_unknown']

    for file in unknown_files:
        df = pd.read_parquet(file, columns=wanted_cols)[wanted_cols]

        if df.empty:
            continue

        validate_table(df, wanted_cols, VAR_CHAR_LIMITS_company_w_unknown, natural_key_columns=['job_link'])

        if not deleted_1:
            delete_s3_prefix(bucket, f'{curated_prefix_w_unknown}')
            deleted_1=True

        df.to_parquet(f'{out_base_unknowns}/part_{m:04d}.parquet', index=False)
        m += 1
