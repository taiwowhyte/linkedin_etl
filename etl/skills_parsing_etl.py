import pandas as pd
from pathlib import Path
from etl.debug_tools_etl import debug_function
from etl.parquet_validation_etl import validate_table
from etl.delete_s3_URI import delete_s3_prefix
import os
import time
import uuid
import tempfile
import boto3
from botocore.config import Config



def build_skills_table(s3_parquet_files, bucket, prefix, run_date):
    seen = set()

    for file in s3_parquet_files:
        print('[skills] READING:', file)   # <-- this is the key line
        s = (pd.read_parquet(file, columns=['job_skills'])['job_skills'].dropna().astype('string').str.strip())

        seen.update(s.tolist())

    skills_df = pd.DataFrame({'job_skills': list(seen)})

    wanted_col = ['job_skills']
    natural_key_col = ['job_skills']
    var_char = {'job_skills':255}
    
    validate_table(skills_df,wanted_col,var_char,natural_key_col)
    
    # Idempotent rerun: delete this run_date partition before writing refreshed output
    delete_s3_prefix(bucket, f'{prefix}/run_date={run_date}/')

    skills_df.to_parquet(f's3://{bucket}/{prefix}/run_date={run_date}/data.parquet', index=False)
    



def _boto3_client():
    # Robust defaults for flaky connections
    return boto3.client('s3', config=Config(retries={'max_attempts': 10, 'mode': 'standard'},connect_timeout=30,read_timeout=300))

def _write_parquet_local_then_upload(df, bucket, key, s3_client, debug: bool = False):
    # Write locally then upload via boto3.upload_file (more reliable than streaming to s3fs)
    fname = f'{os.path.basename(key)}_{uuid.uuid4().hex}.parquet'
    local_path = os.path.join(tempfile.gettempdir(), fname)

    t0 = time.time()
    df.to_parquet(local_path, index=False, compression="snappy")
    if debug:
        print(f'[skills] local parquet wrote: {local_path} rows={len(df)} secs={time.time()-t0:.2f}')

    t1 = time.time()
    s3_client.upload_file(local_path, bucket, key)
    if debug:
        print(f'[skills] uploaded to s3://{bucket}/{key} secs={time.time()-t1:.2f}')

    # Cleanup
    try:
        os.remove(local_path)
    except OSError:
        pass


def cleaning_skills_junction_staging_1(s3_parquet_files,bucket,prefix,wanted_columns,debug=False,batch_size=10,max_rows_per_part=None):

    #Explode job_skills into one row per (job_link, job_skill) and write in parts (streaming)
    #Uses local parquet + boto3 upload for stability on large writes / S3 multipart issues
    prefix = prefix.rstrip('/')
    s3 = _boto3_client()

    n = 1
    buffer = []

    for i, file in enumerate(s3_parquet_files, start=1):
        df = pd.read_parquet(file, columns=wanted_columns)

        #Explode comma-separated skills into one skill per row
        df['job_skills'] = df['job_skills'].astype('string').str.split(',')
        df = df.explode('job_skills')
        df = df.dropna(subset=['job_skills'])

        #Normalise skill text (case and whitespace) and drop empty values
        df['job_skills'] = df['job_skills'].astype('string').str.casefold()
        df['job_skills'] = df['job_skills'].str.replace(r"\s+", ' ', regex=True).str.strip()
        df = df[df['job_skills'] != '']

        # dedup within file
        df = df.drop_duplicates(subset=wanted_columns)

        junction_staging = df[wanted_columns]

        if debug:
            debug_function(junction_staging, True, wanted_columns)

        buffer.append(junction_staging)

        # write every batch_size input files to prevent OOM problems
        if i % batch_size == 0:
            out = pd.concat(buffer, ignore_index=True)
            out = out.drop_duplicates(subset=wanted_columns, ignore_index=True)

            # Make smaller parquet parts if output is massive
            if max_rows_per_part and len(out) > max_rows_per_part:
                start = 0
                while start < len(out):
                    chunk = out.iloc[start:start + max_rows_per_part]
                    key = f'{prefix}/part_{n:05d}.parquet'
                    _write_parquet_local_then_upload(chunk, bucket, key, s3_client=s3, debug=debug)
                    n += 1
                    start += max_rows_per_part
            else:
                key = f'{prefix}/part_{n:05d}.parquet'
                _write_parquet_local_then_upload(out, bucket, key, s3_client=s3, debug=debug)
                n += 1

            buffer = []

    # write leftovers
    if buffer:
        out = pd.concat(buffer, ignore_index=True)
        out = out.drop_duplicates(subset=wanted_columns, ignore_index=True)

        if max_rows_per_part and len(out) > max_rows_per_part:
            start = 0
            while start < len(out):
                chunk = out.iloc[start:start + max_rows_per_part]
                key = f'{prefix}/part_{n:05d}.parquet'
                _write_parquet_local_then_upload(chunk, bucket, key, s3_client=s3, debug=debug)
                n += 1
                start += max_rows_per_part
        else:
            key = f'{prefix}/part_{n:05d}.parquet'
            _write_parquet_local_then_upload(out, bucket, key, s3_client=s3, debug=debug)
            n += 1
