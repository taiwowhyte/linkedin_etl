import pandas as pd
from etl.unknown_placeholders_replace_etl import replace_unknown_placeholders
from etl.delete_s3_URI import delete_s3_prefix

def cleaning_job_title(s3_parquet_files,bucket,prefix,debug=False):
    
    #idempotent rerun
    delete_s3_prefix(bucket, prefix)

    #Common noisy token seen in titles after splitting on hyphen (e.g. 'Engineer - Remote')
    work_pattern = ['part time','full time','2nd shift','prn','virtual/remote','remote','per diem','hybrid',
                    '1st shift','3rd shift','travel contract','part-time','mobile','night shift','nights',
                    'full-time','entry level','onsite','prn/part time']
    employer_name = ["spencer's",'tj maxx','tommy hilfiger',"chico's",'homegoods','calvin klein','the forklift']
    posting_noise = ['broil/grill','rhrp','2+yrs paid tax experience required','shortage control',
                     '$250,000/yearly - $400,000/yearly','fix cars as a mobile mechanic***',
                     'rn at fresenius medical care north america','potential relocation assistance',
                     'manufacturing','level 2','21 and older only','retail','personal financial services',
                     'automotive','us','site)','on bonus','li','employee benefits','07532802']
    dropable_list = work_pattern + employer_name + posting_noise

    #Output schema for the staging table
    output_cols = ['canonical_job_title', 'raw_job_title', 'job_link']

    for i, file in enumerate(s3_parquet_files):
        df = pd.read_parquet(file)[['job_link', 'job_title']].copy()

        series_job_title = df['job_title']
        #Normalise titles so downstream grouping/dedup is consistent
        normalised_series_job_title = (series_job_title.astype('string').str.replace(r'\s+', ' ', regex=True).str.strip().str.casefold())
        # Split on hyphen to separate title vs trailing noise
        before_hypen = normalised_series_job_title.str.split(' *- *', n=1).str[0]
        after_hypen  = normalised_series_job_title.str.split(' *- *', n=1).str[1]

        if debug:
            print(after_hypen.value_counts().head(100)[:50])
            print(after_hypen.value_counts().head(100)[50:100])
        #If one side is “noise” and the other is not, keep the non-noise side as canonical
        condition   = after_hypen.isin(dropable_list)
        condition_2 = before_hypen.isin(dropable_list)

        suffix_only = condition & ~condition_2
        prefix_only = ~condition & condition_2

        canonical = normalised_series_job_title.copy()
        canonical = canonical.mask(suffix_only, before_hypen)
        canonical = canonical.mask(prefix_only, after_hypen)
        
        #Staging output: raw title, canonical title, job_link for joining back to postings
        canonical_raw_staging_df = pd.DataFrame({
            'canonical_job_title': canonical,
            'raw_job_title': normalised_series_job_title,
            'job_link': df['job_link'].astype('string'),
        })

        canonical_raw_staging_df['canonical_job_title'] = replace_unknown_placeholders(
            canonical_raw_staging_df['canonical_job_title']
        )
        canonical_raw_staging_df['canonical_job_title'] = canonical_raw_staging_df['canonical_job_title'].mask(
            canonical_raw_staging_df['canonical_job_title'].isna(),
            'unknown_title'
        )

        canonical_raw_staging_df = canonical_raw_staging_df[output_cols]

        out_path = f's3://{bucket}/{prefix}/part_{i:05d}.parquet'
        canonical_raw_staging_df.to_parquet(out_path, index=False)
