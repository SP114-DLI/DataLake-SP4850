import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="https://sp114api.loclx.io",
    aws_access_key_id="SP114",
    aws_secret_access_key="DataLakeImplementation",
    region_name="us-east-1",
)

def list_buckets():
    """Print all bucket names."""
    response = s3.list_buckets()
    for bucket in response.get("Buckets", []):
        print(bucket["Name"])


def bucket_exists(bucket_name):
    """Return True if bucket exists, else False."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False


def create_bucket(bucket_name):
    """Create a bucket if it does not already exist."""
    if bucket_exists(bucket_name):
        print(f"Bucket '{bucket_name}' already exists.")
        return

    s3.create_bucket(Bucket=bucket_name)
    print(f"Created bucket: {bucket_name}")


# ----------------------------
# OBJECT LISTING
# ----------------------------

def list_objects(bucket_name):
    """List objects in a bucket."""
    try:
        paginator = s3.get_paginator("list_objects_v2")
        found = False

        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                found = True
                print(obj["Key"])

        if not found:
            print(f"Bucket '{bucket_name}' is empty.")

    except ClientError as e:
        print(f"Error listing objects in '{bucket_name}': {e}")


# ----------------------------
# DELETE FUNCTIONS
# ----------------------------

def delete_all_objects(bucket_name):
    """
    Delete all objects in a bucket.
    Required before deleting the bucket itself.
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")
        objects_to_delete = []

        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                objects_to_delete.append({"Key": obj["Key"]})

                # Delete in batches of 1000
                if len(objects_to_delete) == 1000:
                    s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={"Objects": objects_to_delete}
                    )
                    objects_to_delete = []

        # Delete any remaining objects
        if objects_to_delete:
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects_to_delete}
            )

        print(f"All objects deleted from bucket '{bucket_name}'.")

    except ClientError as e:
        print(f"Error deleting objects from '{bucket_name}': {e}")


def delete_bucket(bucket_name, force=False):
    """
    Delete a bucket.
    If force=True, first empties the bucket, then deletes it.
    """
    try:
        if force:
            delete_all_objects(bucket_name)

        s3.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket: {bucket_name}")

    except ClientError as e:
        print(f"Error deleting bucket '{bucket_name}': {e}")


# ----------------------------
# COPY FUNCTIONS
# ----------------------------

def copy_object(source_bucket, source_key, dest_bucket, dest_key=None):
    """
    Copy one object from source bucket to destination bucket.
    If dest_key is None, keeps the same object key.
    """
    if dest_key is None:
        dest_key = source_key

    try:
        copy_source = {
            "Bucket": source_bucket,
            "Key": source_key
        }

        s3.copy_object(
            Bucket=dest_bucket,
            Key=dest_key,
            CopySource=copy_source
        )

        print(f"Copied '{source_key}' from '{source_bucket}' to '{dest_bucket}/{dest_key}'")

    except ClientError as e:
        print(f"Error copying object '{source_key}': {e}")


def copy_all_objects(source_bucket, dest_bucket, prefix=""):
    """
    Copy all objects from one bucket to another.
    Optional prefix lets you copy only part of the bucket.
    """
    try:
        if not bucket_exists(dest_bucket):
            create_bucket(dest_bucket)

        paginator = s3.get_paginator("list_objects_v2")

        found = False
        for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                found = True
                source_key = obj["Key"]

                copy_source = {
                    "Bucket": source_bucket,
                    "Key": source_key
                }

                s3.copy_object(
                    Bucket=dest_bucket,
                    Key=source_key,
                    CopySource=copy_source
                )

                print(f"Copied: {source_key}")

        if not found:
            print(f"No objects found in '{source_bucket}' with prefix '{prefix}'.")

        else:
            print(f"Finished copying objects from '{source_bucket}' to '{dest_bucket}'.")

    except ClientError as e:
        print(f"Error copying objects from '{source_bucket}' to '{dest_bucket}': {e}")


# ----------------------------
# MOVE / CLONE BUCKET CONTENTS
# ----------------------------

def move_all_objects(source_bucket, dest_bucket, prefix=""):

    """
    Move all objects from one bucket to another.
    This copies first, then deletes originals.
    """
    try:
        if not bucket_exists(dest_bucket):
            create_bucket(dest_bucket)

        paginator = s3.get_paginator("list_objects_v2")
        keys_to_delete = []

        for page in paginator.paginate(Bucket=source_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                s3.copy_object(
                    Bucket=dest_bucket,
                    Key=key,
                    CopySource={"Bucket": source_bucket, "Key": key}
                )
                print(f"Copied: {key}")

                keys_to_delete.append({"Key": key})

                if len(keys_to_delete) == 1000:
                    s3.delete_objects(
                        Bucket=source_bucket,
                        Delete={"Objects": keys_to_delete}
                    )
                    keys_to_delete = []

        if keys_to_delete:
            s3.delete_objects(
                Bucket=source_bucket,
                Delete={"Objects": keys_to_delete}
            )

        print(f"Finished moving objects from '{source_bucket}' to '{dest_bucket}'.")

    except ClientError as e:
        print(f"Error moving objects: {e}")

from botocore.exceptions import ClientError

def clear_bucket(bucket_name, prefix=None):
    """
    Delete objects in a bucket.
    
    If prefix is None, deletes all objects.
    If prefix is provided, deletes only objects whose keys start with that prefix.
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")

        delete_batch = []
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix or "")

        found_any = False

        for page in pages:
            contents = page.get("Contents", [])
            for obj in contents:
                found_any = True
                delete_batch.append({"Key": obj["Key"]})

                if len(delete_batch) == 1000:
                    s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={"Objects": delete_batch}
                    )
                    print(f"Deleted batch of {len(delete_batch)} objects")
                    delete_batch = []

        if delete_batch:
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": delete_batch}
            )
            print(f"Deleted final batch of {len(delete_batch)} objects")

        if not found_any:
            print(f"No objects found in bucket '{bucket_name}'"
                  + (f" with prefix '{prefix}'" if prefix else ""))

        else:
            print(f"Finished clearing bucket '{bucket_name}'"
                  + (f" for prefix '{prefix}'" if prefix else ""))

    except ClientError as e:
        print(f"Error clearing bucket '{bucket_name}': {e}")

from botocore.exceptions import ClientError

def delete_all_versions(bucket_name, prefix=None):
    """
    Delete all object versions and delete markers in a versioned bucket.
    
    If prefix is provided, only deletes versions/markers whose keys start with that prefix.
    """
    try:
        paginator = s3.get_paginator("list_object_versions")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix or "")

        batch = []
        found_any = False

        for page in pages:
            versions = page.get("Versions", [])
            delete_markers = page.get("DeleteMarkers", [])

            for obj in versions:
                found_any = True
                batch.append({
                    "Key": obj["Key"],
                    "VersionId": obj["VersionId"]
                })

                if len(batch) == 1000:
                    s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={"Objects": batch}
                    )
                    print(f"Deleted batch of {len(batch)} versions")
                    batch = []

            for marker in delete_markers:
                found_any = True
                batch.append({
                    "Key": marker["Key"],
                    "VersionId": marker["VersionId"]
                })

                if len(batch) == 1000:
                    s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={"Objects": batch}
                    )
                    print(f"Deleted batch of {len(batch)} delete markers")
                    batch = []

        if batch:
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": batch}
            )
            print(f"Deleted final batch of {len(batch)} items")

        if not found_any:
            print(f"No versions or delete markers found in '{bucket_name}'")
        else:
            print(f"Finished deleting all versions/delete markers in '{bucket_name}'")

    except ClientError as e:
        print(f"Error deleting versions in bucket '{bucket_name}': {e}")
"""delete_bucket("automotive-information", force=True)
delete_bucket("images", force=True)
delete_bucket("json-files", force=True)
delete_bucket("text-files", force=True)
delete_bucket("videos", force=True)"""
create_bucket("lakebronze")
create_bucket("lakesilver")
create_bucket("lakegold")