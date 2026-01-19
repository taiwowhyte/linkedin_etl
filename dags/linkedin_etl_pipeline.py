from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.csv_to_parquet import convert_csv_to_parquet
from etl.listing_parquet_files import list_parquet_files
from etl.cleaning_job_posting_staging import cleaning_job_posting_staging
from etl.config import WANTED_COLUMNS, VAR_CHAR_LIMITS, NATURAL_KEY_COLUMNS
from etl.dedup_job_postings_etl import curated_job_posting
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from etl.athena_tables import CREATE_DB, CREATE_CURATED_JOB_POSTING, CREATE_JOB_POSTINGS_SKILLS_JUNCTION, CREATE_SKILLS_TABLE, CREATE_CURATED_SEARCH_CONTEXT,CREATE_JOB_TITLE_STAGING,CREATE_JOB_TITLE,CREATE_JOB_TYPE,CREATE_CURATED_LOCATION,CREATE_JOB_LEVEL,CREATE_CURATED_COMPANY,CREATE_CURATED_COMPANY_W_UNKNOWN
from etl.athena_partitions import ADD_CURATED_JOB_POSTINGS_PARTITION_SQL, ADD_JOB_POSTINGS_SKILLS_JUNCTION_PARTITION_SQL, ADD_SKILLS_TABLE_PARTITION_SQL,ADD_CURATED_SEARCH_CONTEXT_PARTITION_SQL,ADD_JOB_TITLE_STAGING_PARTITION_SQL,ADD_JOB_TITLE_PARTITION_SQL, ADD_JOB_TYPE_PARTITION_SQL,ADD_CURATED_LOCATION_PARTITION_SQL,ADD_JOB_LEVEL_PARTITION_SQL,ADD_CURATED_COMPANY_PARTITION_SQL,ADD_CURATED_COMPANY_W_UNKNOWN_PARTITION_SQL
from etl.skills_parsing_etl import cleaning_skills_junction_staging_1, build_skills_table
from etl.dedup_job_postings_skills_stage import curate_job_postings_skills_stage_streaming
from etl.search_context_build_etl import cleaning_search_context_table
from etl.dedup_search_context import curate_search_context_stage_streaming
from etl.job_title_etl import cleaning_job_title
from etl.dedup_job_title import curate_job_titles_streaming
from etl.job_type_etl import build_job_type
from etl.location_etl import clean_location
from etl.dedup_location_etl import curate_location_streaming
from datetime import timedelta
from etl.job_level_etl import build_job_level
from etl.clean_company_etl import clean_company_parts
from etl.dedup_company_parts_etl import curate_company_streaming
from etl.env import BUCKET, AWS_REGION, ATHENA_WORKGROUP, ATHENA_RESULTS_S3, ATHENA_DB
from etl.env import RAW_ROOT_PREFIX, CLEAN_ROOT_PREFIX, CURATED_ROOT_PREFIX

"""
LinkedIn ETL Airflow DAG

Flow: raw (CSV->Parquet) -> cleaned transforms -> curated/dedup tables -> Athena DDL + partitions.
All datasets are partitioned by Airflow run_date (ds) using the S3 prefix run_date=YYYY-MM-DD
"""

def ingest_task(**run_info):
    convert_csv_to_parquet(**run_info)

bucket = BUCKET


def transform_stage_job_postings(**run_info):
    run_date = run_info['ds']
    prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, prefix)

    clean_prefix = f'{CLEAN_ROOT_PREFIX}/staging_job_postings/run_date={run_date}'
    wanted_cols = WANTED_COLUMNS['job_postings_staging']
    cleaning_job_posting_staging(files, wanted_cols, bucket, clean_prefix, debug=False)


def transform_curate_job_postings(**run_info):
    run_date = run_info['ds']
    clean_prefix = f'{CLEAN_ROOT_PREFIX}/staging_job_postings/run_date={run_date}'
    wanted_cols = WANTED_COLUMNS['job_postings_staging']

    s3_parquet_files = list_parquet_files(bucket, clean_prefix)
    key_columns = NATURAL_KEY_COLUMNS['job_postings_staging']
    var_char = VAR_CHAR_LIMITS['job_postings_staging']
    curated_prefix = f'{CURATED_ROOT_PREFIX}/staging_job_postings'
    curated_job_posting(s3_parquet_files, key_columns, bucket, curated_prefix, wanted_cols, var_char, run_date)


def transform_job_postings_skills_staging(**run_info):
    run_date = run_info['ds']
    prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, prefix)

    clean_prefix = f'{CLEAN_ROOT_PREFIX}/job_postings_skills_staging/run_date={run_date}'
    wanted_cols = WANTED_COLUMNS['job_postings_skills_staging']
    cleaning_skills_junction_staging_1(files, bucket, clean_prefix,wanted_cols, debug=False, batch_size=1,max_rows_per_part=250_000)

def transform_curate_job_postings_skills_staging(**run_info):
    run_date = run_info['ds']
    clean_prefix = f'{CLEAN_ROOT_PREFIX}/job_postings_skills_staging/run_date={run_date}'
    wanted_cols = WANTED_COLUMNS['job_postings_skills_staging']
    s3_parquet_files = list_parquet_files(bucket, clean_prefix)

    key_columns = NATURAL_KEY_COLUMNS['job_postings_skills_staging']
    var_char = VAR_CHAR_LIMITS['job_postings_skills_staging']
    curated_prefix = f'{CURATED_ROOT_PREFIX}/job_postings_skills_stage'
    curate_job_postings_skills_stage_streaming(s3_parquet_files,key_columns,bucket,curated_prefix,wanted_cols,var_char,run_date)

def transform_build_skills_table(**run_info):
    run_date = run_info['ds']
    curated__skill_job_posting_junction_prefix = f'{CURATED_ROOT_PREFIX}/job_postings_skills_stage/run_date={run_date}'
    out_prefix = f'{CURATED_ROOT_PREFIX}/skills'
    files = list_parquet_files(bucket, curated__skill_job_posting_junction_prefix)

    build_skills_table(files, bucket, out_prefix, run_date)

    
def transform_search_context(**run_info):
    run_date = run_info['ds']
    prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, prefix)

    clean_prefix = f'{CLEAN_ROOT_PREFIX}/search_context/run_date={run_date}'
    wanted_cols = WANTED_COLUMNS['search_context']
    cleaning_search_context_table(files, wanted_cols, bucket, clean_prefix, debug=False)

def transform_curate_search_context(**run_info):
    run_date = run_info['ds']
    clean_prefix = f'{CLEAN_ROOT_PREFIX}/search_context/run_date={run_date}'
    wanted_cols = WANTED_COLUMNS['search_context']
    s3_parquet_files = list_parquet_files(bucket, clean_prefix)

    key_columns = NATURAL_KEY_COLUMNS['search_context']
    var_char = VAR_CHAR_LIMITS['search_context']
    curated_prefix = f'{CURATED_ROOT_PREFIX}/search_context'
    curate_search_context_stage_streaming(s3_parquet_files,key_columns,bucket,curated_prefix,wanted_cols,var_char,run_date)

def transform_job_title_table_staging(**run_info):
    run_date = run_info['ds']
    prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, prefix)
    
    clean_prefix = f'{CLEAN_ROOT_PREFIX}/job_title_staging/run_date={run_date}'
    cleaning_job_title(files,bucket,clean_prefix, debug=False) 


def transform_curate_job_title_table(**run_info):
    run_date = run_info['ds']
    clean_prefix = f'{CLEAN_ROOT_PREFIX}/job_title_staging/run_date={run_date}'
    s3_parquet_files = list_parquet_files(bucket, clean_prefix)

    var_char = VAR_CHAR_LIMITS['job_title']
    curated_prefix = f'{CURATED_ROOT_PREFIX}/job_title'
    curate_job_titles_streaming(s3_parquet_files,bucket,curated_prefix,var_char,run_date)


def transform_job_type(**run_info):
    run_date = run_info['ds']
    prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, prefix)

    clean_prefix = f'{CURATED_ROOT_PREFIX}/job_type/run_date={run_date}'
    build_job_type(files, bucket, clean_prefix)

def transform_location(**run_info):
    run_date = run_info['ds']
    prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, prefix)

    clean_prefix = f'{CLEAN_ROOT_PREFIX}/location/run_date={run_date}'
    clean_location(files, bucket, clean_prefix, debug=False)


def transform_curate_location(**run_info):
    run_date = run_info['ds']
    clean_prefix = f'{CLEAN_ROOT_PREFIX}/location/run_date={run_date}'
    files = list_parquet_files(bucket, clean_prefix)
    
    wanted_cols = WANTED_COLUMNS['location']
    keys = NATURAL_KEY_COLUMNS['location']
    var_char = VAR_CHAR_LIMITS['location']
    curated_prefix = f'{CURATED_ROOT_PREFIX}/location/run_date={run_date}'
    curate_location_streaming(files, keys, bucket, curated_prefix, wanted_cols,var_char)

def transform_job_level(**run_info):
    run_date = run_info['ds']
    prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, prefix)

    clean_prefix = f'{CURATED_ROOT_PREFIX}/job_level/run_date={run_date}'
    build_job_level(files, bucket, clean_prefix)

def transform_company(**run_info):
    run_date = run_info['ds']
    clean_prefix = f'{RAW_ROOT_PREFIX}/run_date={run_date}'
    files = list_parquet_files(bucket, clean_prefix)

    clean_prefix = f'{CLEAN_ROOT_PREFIX}/company/run_date={run_date}'
    clean_prefix_w_unknown = f'{CLEAN_ROOT_PREFIX}/company_w_unknown/run_date={run_date}'
    clean_company_parts(files, bucket, clean_prefix, clean_prefix_w_unknown, debug=False)

def transform_curate_company(**run_info):
    run_date = run_info['ds']
    clean_prefix = f'{CLEAN_ROOT_PREFIX}/company/run_date={run_date}'
    clean_prefix_w_unknown = f'{CLEAN_ROOT_PREFIX}/company_w_unknown/run_date={run_date}'
    files = list_parquet_files(bucket,clean_prefix)
    files_w_unknown = list_parquet_files(bucket,clean_prefix_w_unknown)

    curated_prefix = f'{CURATED_ROOT_PREFIX}/company/run_date={run_date}'
    curated_w_unknown = f'{CURATED_ROOT_PREFIX}/company_w_unknown/run_date={run_date}'
    var_char = VAR_CHAR_LIMITS['company']
    var_char_w_unknown = VAR_CHAR_LIMITS['company_w_unknown']
    curate_company_streaming(files,files_w_unknown,bucket,curated_prefix,curated_w_unknown,var_char,var_char_w_unknown)




athena_results = ATHENA_RESULTS_S3

with DAG(
    dag_id='linkedin_etl_pipeline',
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id='load_data',
        python_callable=convert_csv_to_parquet,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )

    transform_stage_job_postings_task = PythonOperator(
        task_id='transform_stage_job_postings',
        python_callable=transform_stage_job_postings,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )

    transform_curated_job_postings_task = PythonOperator(
        task_id='transform_curated_job_postings',
        python_callable=transform_curate_job_postings,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )

    transform_job_postings_skills_staging_task = PythonOperator(
        task_id='transform_job_postings_skills_staging',
        python_callable=transform_job_postings_skills_staging,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )

    transform_curated_job_postings_skills_staging_task = PythonOperator(
        task_id='transform_curated_job_postings_skills_staging',
        python_callable=transform_curate_job_postings_skills_staging,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )


    transform_build_skills_table_task = PythonOperator(
        task_id='transform_build_skills_table',
        python_callable=transform_build_skills_table,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )


    transform_search_context_task = PythonOperator(
        task_id='transform_search_context',
        python_callable=transform_search_context,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )

    transform_curated_search_context_task = PythonOperator(
        task_id='transform_curated_search_context',
        python_callable=transform_curate_search_context,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )


    transform_job_title_table_staging_task = PythonOperator(
        task_id='transform_job_title_staging',
        python_callable=transform_job_title_table_staging,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )

    transform_curated_job_title_table_task = PythonOperator(
        task_id='transform_curated_job_title_table',
        python_callable=transform_curate_job_title_table,
        retries=5,
        retry_delay=timedelta(minutes=2)
    )

    transform_job_type_task = PythonOperator(
        task_id='transform_job_type',
        python_callable=transform_job_type,
        retries=5,
        retry_delay=timedelta(minutes=2),
    )

    transform_location_task = PythonOperator(
        task_id='transform_location',
        python_callable=transform_location,
        retries=5,
        retry_delay=timedelta(minutes=2),       
    )

    transform_curated_location_task = PythonOperator(
        task_id='transform_curated_location',
        python_callable=transform_curate_location,
        retries=5,
        retry_delay=timedelta(minutes=2),
    )

    transform_job_level_task = PythonOperator(
        task_id='transform_job_level',
        python_callable=transform_job_level,
        retries=5,
        retry_delay=timedelta(minutes=2),
    )

    transform_company_task = PythonOperator(
        task_id='transform_company',
        python_callable=transform_company,
        retries=5,
        retry_delay=timedelta(minutes=2),       
    )

    transform_curated_company_task = PythonOperator(
        task_id='transform_curated_company',
        python_callable=transform_curate_company,
        retries=5,
        retry_delay=timedelta(minutes=2),
    )

    create_db_task = AthenaOperator(
        task_id='create_database',
        query=CREATE_DB,
        database='default',
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION
    )

    create_staging_job_postings_task = AthenaOperator(
        task_id='create_staging_job_postings',
        query=CREATE_CURATED_JOB_POSTING,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION
    )

    add_curated_job_postings_partition = AthenaOperator(
        task_id='add_curated_job_postings_partition',
        query=ADD_CURATED_JOB_POSTINGS_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION
    )

    create_job_postings_skills_junction_task = AthenaOperator(
        task_id='create_job_postings_skills_junction',
        query=CREATE_JOB_POSTINGS_SKILLS_JUNCTION,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,
    )


    create_skills_table_task = AthenaOperator(
        task_id='create_skills_table',
        query=CREATE_SKILLS_TABLE,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,       
    )

    add_job_postings_skills_partition_task = AthenaOperator(
        task_id='add_job_postings_skills_partition',
        query=ADD_JOB_POSTINGS_SKILLS_JUNCTION_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,             
    )
    
    add_skills_partition_task = AthenaOperator(
        task_id='add_skills_partition',
        query=ADD_SKILLS_TABLE_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,         
    )

    create_curated_search_context_task = AthenaOperator(
        task_id='create_search_context_table',
        query=CREATE_CURATED_SEARCH_CONTEXT,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,             
    )

    add_search_context_partition_task =  AthenaOperator(
        task_id='add_search_context_partition',
        query=ADD_CURATED_SEARCH_CONTEXT_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION, 
    )

    create_job_title_staging_task = AthenaOperator(
        task_id='create_job_title_staging',
        query=CREATE_JOB_TITLE_STAGING,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,         
    )

    add_job_title_staging_partition_task = AthenaOperator(
        task_id='add_job_title_staging_partition',
        query=ADD_JOB_TITLE_STAGING_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,         
    )


    create_job_title_task = AthenaOperator(
        task_id='create_job_title',
        query=CREATE_JOB_TITLE,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,        
    )

    add_job_title_partition_task = AthenaOperator(
        task_id='add_job_title_partition',
        query=ADD_JOB_TITLE_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,           
    )


    create_job_type_task = AthenaOperator(
        task_id='create_job_type',
        query=CREATE_JOB_TYPE,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,
    )

    add_job_type_partition_task = AthenaOperator(
        task_id='add_job_type_partition',
        query=ADD_JOB_TYPE_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,  
    )

    create_curate_location_task = AthenaOperator(
        task_id='create_curate_location_task',
        query=CREATE_CURATED_LOCATION,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,
    )

    add_curated_location_partition_task = AthenaOperator(
        task_id='add_curated_location_partition',
        query=ADD_CURATED_LOCATION_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,
    )

    create_job_level_task = AthenaOperator(
        task_id='create_job_level',
        query=CREATE_JOB_LEVEL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,
    )

    add_job_level_partition_task = AthenaOperator(
        task_id='add_job_level_partition',
        query=ADD_JOB_LEVEL_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,
    )

    create_curated_company_task = AthenaOperator(
        task_id='create_curated_company',
        query=CREATE_CURATED_COMPANY,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,       
    )

    add_curated_company_partition_task = AthenaOperator(
        task_id='add_company_partition',
        query=ADD_CURATED_COMPANY_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,       
    )


    create_curated_company_w_unknown_task = AthenaOperator(
        task_id='create_curated_company_w_unknown',
        query=CREATE_CURATED_COMPANY_W_UNKNOWN,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,       
    )

    add_curated_company_w_unknown_partition_task = AthenaOperator(
        task_id='add_curated_company_partition',
        query=ADD_CURATED_COMPANY_W_UNKNOWN_PARTITION_SQL,
        database=ATHENA_DB,
        workgroup=ATHENA_WORKGROUP,
        output_location=athena_results,
        region_name=AWS_REGION,       
    )



# DEPENDENCY CHAIN


# 1)Raw ingest first
ingest_task >> [
    transform_stage_job_postings_task,
    transform_job_postings_skills_staging_task,
    transform_search_context_task,
    transform_job_title_table_staging_task,
    transform_job_type_task,
    transform_location_task,
    transform_job_level_task,
    transform_company_task,
]

# 2)Cleaned to curated transforms
transform_stage_job_postings_task >> transform_curated_job_postings_task

transform_job_postings_skills_staging_task >> transform_curated_job_postings_skills_staging_task
transform_curated_job_postings_skills_staging_task >> transform_build_skills_table_task

transform_search_context_task >> transform_curated_search_context_task

transform_job_title_table_staging_task >> transform_curated_job_title_table_task

transform_location_task >> transform_curated_location_task

transform_company_task >> transform_curated_company_task

# 3)Athena DB must exist before any CREATE TABLE
create_db_task >> [
    create_staging_job_postings_task,
    create_job_postings_skills_junction_task,
    create_skills_table_task,
    create_curated_search_context_task,
    create_job_title_staging_task,
    create_job_title_task,
    create_job_type_task,
    create_curate_location_task,
    create_job_level_task,
    create_curated_company_task,
    create_curated_company_w_unknown_task,
]

# 4)Add partitions only after both parquet and table exist

#job_postings (curated)
[transform_curated_job_postings_task, create_staging_job_postings_task] >> add_curated_job_postings_partition

#job_postings_skills_junction
[transform_curated_job_postings_skills_staging_task, create_job_postings_skills_junction_task] >> add_job_postings_skills_partition_task

#skills
[transform_build_skills_table_task, create_skills_table_task] >> add_skills_partition_task

#search_context
[transform_curated_search_context_task, create_curated_search_context_task] >> add_search_context_partition_task

#job_title staging + curated
[transform_job_title_table_staging_task, create_job_title_staging_task] >> add_job_title_staging_partition_task
[transform_curated_job_title_table_task, create_job_title_task] >> add_job_title_partition_task

#job_type
[transform_job_type_task, create_job_type_task] >> add_job_type_partition_task

#location
[transform_curated_location_task, create_curate_location_task] >> add_curated_location_partition_task

# job_level
[transform_job_level_task, create_job_level_task] >> add_job_level_partition_task

# company + company_w_unknown
[transform_curated_company_task, create_curated_company_task] >> add_curated_company_partition_task
[transform_curated_company_task, create_curated_company_w_unknown_task] >> add_curated_company_w_unknown_partition_task
