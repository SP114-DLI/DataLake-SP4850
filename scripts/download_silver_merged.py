"""Download all parquet files from the silver bucket and merge into one local file."""

import os
import tempfile
import pyarrow.parquet as pq
import pyarrow as pa
from s3 import get_client
from raw.config import BUCKET_SILVER

OUTPUT_FILE = "downloads/silver_merged.parquet"


def list_parquet_keys(s3, bucket):
    """List all .parquet object keys in a bucket."""
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def download_and_merge(output=OUTPUT_FILE):
    s3 = get_client()
    keys = list_parquet_keys(s3, BUCKET_SILVER)

    if not keys:
        print("No parquet files found in the silver bucket.")
        return

    print(f"Found {len(keys)} parquet file(s) in '{BUCKET_SILVER}':")
    for k in keys:
        print(f"  {k}")

    tables = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for key in keys:
            local_path = os.path.join(tmpdir, os.path.basename(key))
            print(f"Downloading {key}...")
            s3.download_file(BUCKET_SILVER, key, local_path)
            tables.append(pq.read_table(local_path))

    merged = pa.concat_tables(tables, promote_options="default")
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    pq.write_table(merged, output)
    print(f"\nMerged {len(tables)} file(s) -> {output}  ({merged.num_rows} rows, {merged.num_columns} columns)")


if __name__ == "__main__":
    download_and_merge()
