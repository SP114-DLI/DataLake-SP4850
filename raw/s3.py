"""S3-compatible object storage operations via boto3."""

import boto3
from botocore.exceptions import ClientError

from config import ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION, USE_HTTPS


def get_client(endpoint=None, access_key=None, secret_key=None, region=None, use_https=None):
    """Create a configured boto3 S3 client."""
    endpoint = endpoint or ENDPOINT
    access_key = access_key or ACCESS_KEY
    secret_key = secret_key or SECRET_KEY
    region = region or REGION
    use_https = use_https if use_https is not None else USE_HTTPS
    protocol = "https" if use_https else "http"

    return boto3.client(
        "s3",
        endpoint_url=f"{protocol}://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


# Lazy default client
_default_client = None


def _client():
    """Get or create the default S3 client."""
    global _default_client
    if _default_client is None:
        _default_client = get_client()
    return _default_client


def list_buckets(client=None):
    """Print all bucket names."""
    s3 = client or _client()
    for bucket in s3.list_buckets().get("Buckets", []):
        print(bucket["Name"])


def bucket_exists(bucket_name, client=None):
    """Return True if bucket exists."""
    s3 = client or _client()
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False


def create_bucket(bucket_name, client=None):
    """Create a bucket if it doesn't exist."""
    s3 = client or _client()
    if bucket_exists(bucket_name, s3):
        print(f"Bucket '{bucket_name}' already exists.")
        return
    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket: {bucket_name}")


def list_objects(bucket_name, client=None):
    """List and print object keys in a bucket."""
    s3 = client or _client()
    try:
        found = False
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                found = True
                print(obj["Key"])
        if not found:
            print(f"Bucket '{bucket_name}' is empty.")
    except ClientError as e:
        print(f"Error listing objects in '{bucket_name}': {e}")


def _delete_batch(s3, bucket_name, batch):
    """Delete a batch of objects from a bucket."""
    if batch:
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})


def delete_all_objects(bucket_name, client=None):
    """Delete all objects in a bucket."""
    s3 = client or _client()
    try:
        batch = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                batch.append({"Key": obj["Key"]})
                if len(batch) == 1000:
                    _delete_batch(s3, bucket_name, batch)
                    batch = []
        _delete_batch(s3, bucket_name, batch)
        print(f"All objects deleted from '{bucket_name}'.")
    except ClientError as e:
        print(f"Error deleting objects from '{bucket_name}': {e}")


def delete_bucket(bucket_name, force=False, client=None):
    """Delete a bucket. If force=True, empties it first."""
    s3 = client or _client()
    try:
        if force:
            delete_all_objects(bucket_name, s3)
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket: {bucket_name}")
    except ClientError as e:
        print(f"Error deleting bucket '{bucket_name}': {e}")


def copy_object(source_bucket, source_key, dest_bucket, dest_key=None, client=None):
    """Copy an object between buckets."""
    s3 = client or _client()
    dest_key = dest_key or source_key
    try:
        s3.copy_object(
            Bucket=dest_bucket, Key=dest_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )
        print(f"Copied '{source_key}' -> '{dest_bucket}/{dest_key}'")
    except ClientError as e:
        print(f"Error copying '{source_key}': {e}")


def copy_all_objects(source_bucket, dest_bucket, prefix="", client=None):
    """Copy all objects from one bucket to another."""
    s3 = client or _client()
    try:
        if not bucket_exists(dest_bucket, s3):
            create_bucket(dest_bucket, s3)
        found = False
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=source_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                found = True
                key = obj["Key"]
                s3.copy_object(
                    Bucket=dest_bucket, Key=key,
                    CopySource={"Bucket": source_bucket, "Key": key},
                )
                print(f"Copied: {key}")
        if not found:
            print(f"No objects in '{source_bucket}' with prefix '{prefix}'.")
        else:
            print(f"Finished copying from '{source_bucket}' to '{dest_bucket}'.")
    except ClientError as e:
        print(f"Error copying: {e}")


def move_all_objects(source_bucket, dest_bucket, prefix="", client=None):
    """Move all objects from one bucket to another (copy then delete)."""
    s3 = client or _client()
    try:
        if not bucket_exists(dest_bucket, s3):
            create_bucket(dest_bucket, s3)
        to_delete = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=source_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                s3.copy_object(
                    Bucket=dest_bucket, Key=key,
                    CopySource={"Bucket": source_bucket, "Key": key},
                )
                to_delete.append({"Key": key})
                if len(to_delete) == 1000:
                    _delete_batch(s3, source_bucket, to_delete)
                    to_delete = []
        _delete_batch(s3, source_bucket, to_delete)
        print(f"Moved objects from '{source_bucket}' to '{dest_bucket}'.")
    except ClientError as e:
        print(f"Error moving objects: {e}")


def clear_bucket(bucket_name, prefix=None, client=None):
    """Delete objects in a bucket, optionally filtered by prefix."""
    s3 = client or _client()
    try:
        batch = []
        found = False
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket_name, Prefix=prefix or ""):
            for obj in page.get("Contents", []):
                found = True
                batch.append({"Key": obj["Key"]})
                if len(batch) == 1000:
                    _delete_batch(s3, bucket_name, batch)
                    batch = []
        _delete_batch(s3, bucket_name, batch)
        suffix = f" prefix '{prefix}'" if prefix else ""
        print(f"{'Cleared' if found else 'No objects in'} '{bucket_name}'{suffix}")
    except ClientError as e:
        print(f"Error clearing '{bucket_name}': {e}")


def delete_all_versions(bucket_name, prefix=None, client=None):
    """Delete all object versions and delete markers in a versioned bucket."""
    s3 = client or _client()
    try:
        batch = []
        found = False
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket_name, Prefix=prefix or ""):
            for obj in page.get("Versions", []) + page.get("DeleteMarkers", []):
                found = True
                batch.append({"Key": obj["Key"], "VersionId": obj["VersionId"]})
                if len(batch) == 1000:
                    _delete_batch(s3, bucket_name, batch)
                    batch = []
        _delete_batch(s3, bucket_name, batch)
        if found:
            print(f"Deleted all versions in '{bucket_name}'.")
        else:
            print(f"No versions found in '{bucket_name}'.")
    except ClientError as e:
        print(f"Error deleting versions in '{bucket_name}': {e}")


def setup_lake_buckets(client=None):
    """Create the data lake buckets (silver, gold). Raw/bronze is lakeraw."""
    from raw.config import BUCKET_SILVER, BUCKET_GOLD
    for bucket in [BUCKET_SILVER, BUCKET_GOLD]:
        create_bucket(bucket, client)


if __name__ == "__main__":
    delete_all_versions("lakebronze")
    delete_bucket("lakebronze", force=True)
    list_buckets()

