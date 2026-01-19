import boto3

def list_parquet_files(bucket, prefix):
    """
    List all Parquet objects under an S3 prefix and return them as s3:// URIs

    - S3 listing is paginated (max ~1000 keys per response), so loop with ContinuationToken
    - Filter for .parquet files because partitions/prefixes can contain non-data artifacts
    - Sort the results to keep processing deterministic (useful for debugging and repeatable runs)

    
    :param bucket: S3 bucket name
    :param prefix: Key prefix to list under
    """

    #continuation token for paginated S3 files
    token = None
    s3 = boto3.client('s3')
    files = []

    while True:
         #list_objects_v2 returns up to 1000 keys. ContinuationToken used to fetch next page
        if token is None:
            resp = s3.list_objects_v2(Bucket=bucket,Prefix=prefix)
        else:
            resp = s3.list_objects_v2(Bucket=bucket,Prefix=prefix, ContinuationToken=token)
        
        #'Contents' is absent if the prefix has no objects, so default to []
        for item in resp.get('Contents',[]):
            key = item['Key']
            if key.endswith('.parquet'):
                files.append(f's3://{bucket}/{key}')

        #if truncated, keep going through pages, otherwise stop
        if resp.get('IsTruncated'):
            token = resp.get('NextContinuationToken')
        else:
            
            break
    #sorted for deterministic ordering
    return sorted(files)
