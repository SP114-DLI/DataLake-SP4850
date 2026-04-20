"""
Silver layer data description.

Connects to the silver bucket, reads all Parquet files, and prints
a comprehensive description: shape, schema, pandas describe() for
numeric and string columns, and value-frequency summaries for key
categorical fields.

Usage:
    python -m silver.silver_description
    python -m silver.silver_description --out silver_description.json
"""

import argparse
import json
import logging
import tempfile
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError

from raw.config import (
    ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION, USE_HTTPS,
    BUCKET_SILVER,
)

logger = logging.getLogger(__name__)

QUARANTINE_PREFIX = "_quarantine/"


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _get_s3(endpoint=None, access_key=None, secret_key=None,
            region=None, use_https=None):
    endpoint = endpoint or ENDPOINT
    access_key = access_key or ACCESS_KEY
    secret_key = secret_key or SECRET_KEY
    region = region or REGION
    use_https = use_https if use_https is not None else USE_HTTPS
    proto = "https" if use_https else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{proto}://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(s3={"addressing_style": "path"}),
    )


def _list_keys(s3, bucket, suffix="", prefix=""):
    keys = []
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(suffix):
                    keys.append(obj["Key"])
    except ClientError:
        pass
    return sorted(keys)


def _download_parquet(s3, bucket, key):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    s3.download_file(bucket, key, str(tmp_path))
    df = pd.read_parquet(tmp_path)
    tmp_path.unlink()
    return df


# ---------------------------------------------------------------------------
# Description helpers
# ---------------------------------------------------------------------------

def _safe(val):
    """Convert numpy/pandas scalars to JSON-safe Python types."""
    if val is None or pd.isna(val):
        return None
    if hasattr(val, "item"):
        return val.item()
    return val


def describe_silver(s3=None, bucket=None):
    """
    Build a data description dict for the silver layer.

    Includes:
      - shape (rows, columns, files)
      - schema (column name -> dtype)
      - numeric_summary (pandas describe transpose)
      - string_summary (non-null count, unique, top values)
      - value_counts for key categorical columns
    """
    s3 = s3 or _get_s3()
    bucket = bucket or BUCKET_SILVER

    silver_keys = [
        k for k in _list_keys(s3, bucket, suffix=".parquet")
        if not k.startswith(QUARANTINE_PREFIX) and not k.startswith("_meta/")
    ]

    if not silver_keys:
        logger.warning("No silver Parquet files found in '%s'", bucket)
        return {"error": "No silver data found"}

    frames = []
    for key in silver_keys:
        logger.info("Reading %s", key)
        try:
            frames.append(_download_parquet(s3, bucket, key))
        except Exception as e:
            logger.error("Failed to read %s: %s", key, e)

    if not frames:
        return {"error": "Could not read any silver files"}

    df = pd.concat(frames, ignore_index=True)
    total_rows, total_cols = df.shape
    logger.info("Loaded %d rows x %d columns from %d files",
                total_rows, total_cols, len(frames))

    # --- Schema ---
    schema = {col: str(df[col].dtype) for col in df.columns}

    # --- Numeric describe ---
    numeric_cols = df.select_dtypes(include="number")
    numeric_summary = {}
    if not numeric_cols.empty:
        desc = numeric_cols.describe().T
        for col in desc.index:
            numeric_summary[col] = {
                k: _safe(v) for k, v in desc.loc[col].items()
            }
            numeric_summary[col]["null_count"] = int(df[col].isna().sum())
            numeric_summary[col]["null_pct"] = round(
                df[col].isna().sum() / total_rows * 100, 2
            ) if total_rows else 0.0

    # --- String describe ---
    string_cols = [
        c for c in df.columns
        if str(df[c].dtype) in ("string", "object") and not c.startswith("_")
    ]
    string_summary = {}
    for col in string_cols:
        non_null = df[col].dropna()
        non_empty = non_null[non_null.astype(str).str.strip() != ""]
        try:
            nunique = non_empty.nunique()
        except TypeError:
            nunique = None
        top_vals = (
            non_empty.value_counts().head(10).to_dict()
            if nunique and nunique > 0 else {}
        )
        string_summary[col] = {
            "non_null_count": int(len(non_null)),
            "non_empty_count": int(len(non_empty)),
            "null_count": int(total_rows - len(non_null)),
            "null_pct": round((total_rows - len(non_null)) / total_rows * 100, 2)
            if total_rows else 0.0,
            "unique_count": _safe(nunique),
            "top_values": {str(k): int(v) for k, v in top_vals.items()},
        }

    # --- Key categorical value counts ---
    cat_fields = [
        "make", "model", "bodytype", "fuel", "drivetype",
        "transmission", "vehicle_condition", "dealer_state",
        "accident_history", "owner_history",
    ]
    value_counts = {}
    for col in cat_fields:
        if col in df.columns:
            vc = df[col].value_counts(dropna=False).head(20)
            value_counts[col] = {
                str(k) if pd.notna(k) else "<null>": int(v)
                for k, v in vc.items()
            }

    description = {
        "shape": {
            "rows": total_rows,
            "columns": total_cols,
            "files": len(silver_keys),
        },
        "schema": schema,
        "numeric_summary": numeric_summary,
        "string_summary": string_summary,
        "value_counts": value_counts,
    }

    return description


# ---------------------------------------------------------------------------
# Pretty-print
# ---------------------------------------------------------------------------

def print_description(desc):
    shape = desc.get("shape", {})
    print("=" * 70)
    print("  SILVER LAYER DATA DESCRIPTION")
    print(f"  Rows: {shape.get('rows', 0):,}  |  "
          f"Columns: {shape.get('columns', 0)}  |  "
          f"Files: {shape.get('files', 0)}")
    print("=" * 70)

    # Schema
    print("\n  SCHEMA")
    print(f"  {'Column':<35} {'Dtype':<15}")
    print("  " + "-" * 50)
    for col, dtype in desc.get("schema", {}).items():
        print(f"  {col:<35} {dtype:<15}")

    # Numeric summary
    num = desc.get("numeric_summary", {})
    if num:
        print("\n  NUMERIC SUMMARY")
        print(f"  {'Column':<28} {'Count':>8} {'Mean':>12} "
              f"{'Std':>12} {'Min':>12} {'Max':>12} {'Null%':>7}")
        print("  " + "-" * 93)
        for col, stats in num.items():
            count = stats.get("count", 0)
            mean = stats.get("mean")
            std = stats.get("std")
            mn = stats.get("min")
            mx = stats.get("max")
            np_ = stats.get("null_pct", 0)
            mean_s = f"{mean:>12.2f}" if mean is not None else f"{'N/A':>12}"
            std_s = f"{std:>12.2f}" if std is not None else f"{'N/A':>12}"
            min_s = f"{mn:>12.2f}" if mn is not None else f"{'N/A':>12}"
            max_s = f"{mx:>12.2f}" if mx is not None else f"{'N/A':>12}"
            print(f"  {col:<28} {count:>8.0f} "
                  f"{mean_s} {std_s} {min_s} {max_s} {np_:>6.1f}%")

    # String summary
    string = desc.get("string_summary", {})
    if string:
        print("\n  STRING COLUMNS")
        print(f"  {'Column':<30} {'NonNull':>8} {'Unique':>8} {'Null%':>7}")
        print("  " + "-" * 56)
        for col, info in string.items():
            nn = info.get("non_null_count", 0)
            uq = info.get("unique_count", "?")
            np_ = info.get("null_pct", 0)
            print(f"  {col:<30} {nn:>8,} {str(uq):>8} {np_:>6.1f}%")

    # Value counts
    vc = desc.get("value_counts", {})
    if vc:
        print("\n  KEY CATEGORICAL DISTRIBUTIONS")
        for col, counts in vc.items():
            print(f"\n  {col}:")
            for val, cnt in counts.items():
                print(f"    {val:<30} {cnt:>8,}")

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Silver layer data description")
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--access-key", default=None)
    parser.add_argument("--secret-key", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--no-https", action="store_true")
    parser.add_argument("--out", default=None, help="Save JSON description to file")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(message)s")

    s3 = _get_s3(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        use_https=False if args.no_https else None,
    )

    desc = describe_silver(s3=s3, bucket=args.bucket)
    print_description(desc)

    if args.out:
        with open(args.out, "w") as f:
            json.dump(desc, f, indent=2)
        print(f"Description saved to {args.out}")


if __name__ == "__main__":
    main()
