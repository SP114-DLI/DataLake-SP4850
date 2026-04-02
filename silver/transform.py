"""Silver layer transformation: JSONL from raw bucket -> deduplicated Parquet in silver bucket."""

import io
import json
import logging
import tempfile
import argparse
from pathlib import Path
import pandas as pd

from raw.config import (
    ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION, USE_HTTPS,
    BUCKET_RAW, BUCKET_SILVER,
)
from raw.s3 import get_client, bucket_exists, create_bucket

logger = logging.getLogger(__name__)


def _setup_logging(level="INFO"):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper()))


def list_raw_jsonl(client, bucket=None):
    """List all .jsonl object keys in the raw bucket."""
    bucket = bucket or BUCKET_RAW
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                keys.append(obj["Key"])
    return sorted(keys)


def list_silver_parquet(client, bucket=None):
    """List all .parquet object keys already in the silver bucket."""
    bucket = bucket or BUCKET_SILVER
    keys = set()
    if not bucket_exists(bucket, client):
        return keys
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.add(obj["Key"])
    return keys


def _parquet_key(jsonl_key):
    """Convert a JSONL filename to its corresponding Parquet key."""
    return jsonl_key.replace(".jsonl", ".parquet")


def download_jsonl(client, bucket, key):
    """Download a JSONL file from S3 and return a list of dicts."""
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    records = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def flatten_record(record):
    """Flatten nested dealer and monthlyPaymentEstimate fields into top-level columns."""
    flat = {}
    for key, value in record.items():
        if key == "dealer" and isinstance(value, dict):
            for dk, dv in value.items():
                flat[f"dealer_{dk}"] = dv
        elif key == "monthlyPaymentEstimate" and isinstance(value, dict):
            for mk, mv in value.items():
                flat[f"monthly_{mk}"] = mv
        elif isinstance(value, list):
            flat[key] = json.dumps(value)
        else:
            flat[key] = value
    return flat


def records_to_dataframe(records):
    """Convert raw JSON records to a flattened, deduplicated DataFrame."""
    flat = [flatten_record(r) for r in records]
    df = pd.DataFrame(flat)
    if df.empty:
        return df
    # Deduplicate by VIN if present, keeping first occurrence
    if "vin" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["vin"], keep="first")
        dupes = before - len(df)
        if dupes:
            logger.info(f"  Removed {dupes} duplicate VINs ({before} -> {len(df)})")
    return df


def upload_parquet(client, bucket, key, df):
    """Write a DataFrame as Parquet and upload to S3."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    df.to_parquet(tmp_path, engine="pyarrow", index=False)
    size_mb = tmp_path.stat().st_size / (1024 ** 2)
    client.upload_file(str(tmp_path), bucket, key)
    tmp_path.unlink()
    return size_mb


def transform_all(client=None, raw_bucket=None, silver_bucket=None, skip_existing=True):
    """
    Download all JSONL from raw, convert to Parquet, upload to silver.

    Returns:
        dict with transformation stats
    """
    client = client or get_client()
    raw_bucket = raw_bucket or BUCKET_RAW
    silver_bucket = silver_bucket or BUCKET_SILVER

    if not bucket_exists(raw_bucket, client):
        logger.error(f"Raw bucket '{raw_bucket}' does not exist.")
        return {"success": False, "error": f"Bucket '{raw_bucket}' not found"}

    if not bucket_exists(silver_bucket, client):
        create_bucket(silver_bucket, client)
        logger.info(f"Created silver bucket '{silver_bucket}'")

    jsonl_keys = list_raw_jsonl(client, raw_bucket)
    if not jsonl_keys:
        logger.warning("No JSONL files found in raw bucket.")
        return {"success": True, "files_processed": 0}

    existing_parquet = list_silver_parquet(client, silver_bucket) if skip_existing else set()

    stats = {
        "success": True,
        "files_processed": 0,
        "files_skipped": 0,
        "files_failed": 0,
        "total_records": 0,
        "total_after_dedup": 0,
        "files": [],
    }

    for jsonl_key in jsonl_keys:
        parquet_key = _parquet_key(jsonl_key)

        if skip_existing and parquet_key in existing_parquet:
            logger.info(f"Skipping (already exists): {parquet_key}")
            stats["files_skipped"] += 1
            continue

        logger.info(f"Processing: {jsonl_key}")
        try:
            records = download_jsonl(client, raw_bucket, jsonl_key)
            logger.info(f"  Downloaded {len(records)} records")

            df = records_to_dataframe(records)
            if df.empty:
                logger.warning(f"  No records after processing, skipping upload.")
                stats["files_skipped"] += 1
                continue

            size_mb = upload_parquet(client, silver_bucket, parquet_key, df)
            logger.info(f"  Uploaded {parquet_key} ({size_mb:.2f} MB, {len(df)} rows)")

            stats["files_processed"] += 1
            stats["total_records"] += len(records)
            stats["total_after_dedup"] += len(df)
            stats["files"].append({
                "source": jsonl_key,
                "dest": parquet_key,
                "raw_records": len(records),
                "deduped_records": len(df),
                "size_mb": round(size_mb, 2),
            })
        except Exception as e:
            logger.error(f"  Failed to process {jsonl_key}: {e}")
            stats["files_failed"] += 1
            stats["files"].append({"source": jsonl_key, "error": str(e)})

    duped = stats["total_records"] - stats["total_after_dedup"]
    logger.info(
        f"Done: {stats['files_processed']} processed, {stats['files_skipped']} skipped, "
        f"{stats['files_failed']} failed. {stats['total_records']} records -> "
        f"{stats['total_after_dedup']} after dedup ({duped} duplicates removed)"
    )
    stats["success"] = stats["files_failed"] == 0
    return stats


def main():
    parser = argparse.ArgumentParser(description="Transform raw JSONL to silver Parquet")
    parser.add_argument("--endpoint", default=None, help="S3 endpoint")
    parser.add_argument("--access-key", default=None, help="Access key")
    parser.add_argument("--secret-key", default=None, help="Secret key")
    parser.add_argument("--raw-bucket", default=None, help="Raw bucket name")
    parser.add_argument("--silver-bucket", default=None, help="Silver bucket name")
    parser.add_argument("--no-skip", action="store_true", help="Re-process existing files")
    parser.add_argument("--no-https", action="store_true", help="Use HTTP")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

    args = parser.parse_args()
    _setup_logging(args.log_level)

    client = get_client(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        use_https=False if args.no_https else None,
    )

    result = transform_all(
        client=client,
        raw_bucket=args.raw_bucket,
        silver_bucket=args.silver_bucket,
        skip_existing=not args.no_skip,
    )

    results_file = Path("silver_transform_results.json")
    with open(results_file, "w") as f:
        json.dump(result, f, indent=2)
    logger.info(f"Results saved to {results_file}")

    return 0 if result["success"] else 1


if __name__ == "__main__":
    exit(main())
