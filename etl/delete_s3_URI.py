import boto3

def delete_s3_prefix(bucket, prefix):
    """
    Delete all objects under an S3 prefix
    
    Purpose:
    - Deleting a partition before rewriting it makes reruns idempotent
      (prevents mixing old and new parquet parts under the same run_date)

    Implementation notes:
    - Uses a paginator because listing can exceed 1000 keys.
    - Deletes in batches of 1000 because delete_objects has a 1000-key limit per request.

    :param bucket: S3 bucket name
    :param prefix: Key prefix to delete under 

    Returns:Number of objects *attempted* to delete.
    """
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    deleted = 0
    batch = []

    #Paginate through all objects that match the prefix
    for page in paginator.paginate(bucket, prefix):
        for obj in page.get('Contents', []):
            batch.append({'Key': obj['Key']})
            #S3 delete_objects limit: max 1000 objects per request
            if len(batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={'Objects': batch})
                deleted += 1000
                batch = []
    #Delete any remaining keys that didn't fill a full batch
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={'Objects': batch})
        deleted += len(batch)

    return deleted
