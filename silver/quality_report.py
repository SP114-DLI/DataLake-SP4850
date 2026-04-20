"""
Silver layer data quality report.

Reads all Parquet files from the silver bucket and produces per-column
statistics on null/missing values, plus quarantine summaries.

Usage:
    python -m silver.quality_report                 # print to stdout
    python -m silver.quality_report --out report.json  # save JSON report
"""

import argparse
import io
import json
import logging
import tempfile
from collections import Counter
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
# Report generation
# ---------------------------------------------------------------------------

def generate_report(s3=None, bucket=None):
    """
    Build a quality report across all silver Parquet files.

    Returns a dict with:
      - total_records, total_files
      - per-column: null_count, null_pct, dtype, unique_count, sample_values
      - quarantine summary: total quarantined, reason breakdown
    """
    s3 = s3 or _get_s3()
    bucket = bucket or BUCKET_SILVER

    # ---- Silver data files (exclude quarantine and meta) ----
    silver_keys = [
        k for k in _list_keys(s3, bucket, suffix=".parquet")
        if not k.startswith(QUARANTINE_PREFIX) and not k.startswith("_meta/")
    ]

    if not silver_keys:
        logger.warning("No silver Parquet files found in '%s'", bucket)
        return {"error": "No silver data found", "total_records": 0}

    # Read and concatenate all silver files
    frames = []
    for key in silver_keys:
        logger.info("Reading %s", key)
        try:
            frames.append(_download_parquet(s3, bucket, key))
        except Exception as e:
            logger.error("Failed to read %s: %s", key, e)

    if not frames:
        return {"error": "Could not read any silver files", "total_records": 0}

    df = pd.concat(frames, ignore_index=True)
    total = len(df)
    logger.info("Total silver records: %d across %d files", total, len(frames))

    # ---- Per-column null/missing stats ----
    columns = {}
    for col in sorted(df.columns):
        null_count = int(df[col].isna().sum())

        # Also count empty strings for string columns
        empty_count = 0
        if df[col].dtype == "object" or str(df[col].dtype) == "string":
            empty_count = int((df[col].fillna("").astype(str).str.strip() == "").sum() - null_count)
            if empty_count < 0:
                empty_count = 0

        total_missing = null_count + empty_count
        null_pct = round(total_missing / total * 100, 2) if total > 0 else 0.0

        col_info = {
            "null_count": null_count,
            "empty_string_count": empty_count,
            "total_missing": total_missing,
            "missing_pct": null_pct,
            "dtype": str(df[col].dtype),
            "non_null_count": total - null_count,
        }

        # Add sample unique values for non-metadata string columns
        if not col.startswith("_") and (df[col].dtype == "object" or str(df[col].dtype) == "string"):
            try:
                nunique = df[col].nunique()
                col_info["unique_count"] = int(nunique)
                samples = df[col].dropna().unique()[:5].tolist()
                col_info["sample_values"] = samples
            except TypeError:
                # Column contains unhashable types (dicts/lists)
                non_null = df[col].dropna()
                col_info["unique_count"] = None
                col_info["sample_values"] = [str(v) for v in non_null.head(5).tolist()]

        # Add basic numeric stats
        if pd.api.types.is_numeric_dtype(df[col]):
            desc = df[col].describe()
            col_info["min"] = _safe_num(desc.get("min"))
            col_info["max"] = _safe_num(desc.get("max"))
            col_info["mean"] = _safe_num(desc.get("mean"))
            col_info["median"] = _safe_num(df[col].median())

        columns[col] = col_info

    # ---- Quarantine summary ----
    quarantine_keys = _list_keys(s3, bucket, suffix=".parquet",
                                 prefix=QUARANTINE_PREFIX)
    quarantine_total = 0
    reason_counts = Counter()

    for key in quarantine_keys:
        try:
            qdf = _download_parquet(s3, bucket, key)
            quarantine_total += len(qdf)
            if "_rejection_reason" in qdf.columns:
                for reasons_str in qdf["_rejection_reason"].dropna():
                    for reason in reasons_str.rstrip(";").split(";"):
                        reason = reason.strip()
                        if reason:
                            reason_counts[reason] += 1
        except Exception as e:
            logger.error("Failed to read quarantine file %s: %s", key, e)

    # ---- Duplicate check (VINs appearing in multiple files) ----
    vin_dupes = 0
    if "vin" in df.columns:
        vin_dupes = int(df["vin"].duplicated().sum())

    # ---- Build report ----
    report = {
        "total_records": total,
        "total_files": len(silver_keys),
        "duplicate_vins_in_silver": vin_dupes,
        "quarantine": {
            "total_rejected": quarantine_total,
            "quarantine_files": len(quarantine_keys),
            "rejection_reasons": dict(reason_counts.most_common()),
        },
        "columns": columns,
        "columns_fully_populated": [
            c for c, info in columns.items() if info["total_missing"] == 0
        ],
        "columns_mostly_null": [
            c for c, info in columns.items() if info["missing_pct"] > 50
        ],
    }

    return report


def _safe_num(val):
    """Convert numpy/pandas numeric to Python float, handling NA."""
    if val is None or pd.isna(val):
        return None
    return round(float(val), 4)


# ---------------------------------------------------------------------------
# Pretty-print for terminal
# ---------------------------------------------------------------------------

def print_report(report):
    """Print a human-readable quality summary."""
    print("=" * 70)
    print(f"  SILVER LAYER QUALITY REPORT")
    print(f"  Records: {report['total_records']:,}  |  Files: {report.get('total_files', '?')}")
    print(f"  Duplicate VINs in silver: {report.get('duplicate_vins_in_silver', 0)}")
    print("=" * 70)

    q = report.get("quarantine", {})
    if q.get("total_rejected", 0) > 0:
        print(f"\n  QUARANTINE: {q['total_rejected']:,} rejected records")
        for reason, count in q.get("rejection_reasons", {}).items():
            print(f"    - {reason}: {count:,}")
    else:
        print("\n  QUARANTINE: 0 rejected records")

    print(f"\n  {'Column':<30} {'Missing':>8} {'Pct':>7} {'Type':<10}")
    print("  " + "-" * 60)

    cols = report.get("columns", {})
    # Sort: highest missing % first
    for col in sorted(cols, key=lambda c: -cols[c]["missing_pct"]):
        info = cols[col]
        missing = info["total_missing"]
        pct = info["missing_pct"]
        dtype = info["dtype"]
        marker = " ***" if pct > 50 else ""
        print(f"  {col:<30} {missing:>8,} {pct:>6.1f}% {dtype:<10}{marker}")

    if report.get("columns_fully_populated"):
        print(f"\n  Fully populated ({len(report['columns_fully_populated'])}): "
              f"{', '.join(report['columns_fully_populated'][:10])}"
              f"{'...' if len(report['columns_fully_populated']) > 10 else ''}")

    if report.get("columns_mostly_null"):
        print(f"  Mostly null >50% ({len(report['columns_mostly_null'])}): "
              f"{', '.join(report['columns_mostly_null'])}")

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Silver data quality report")
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--access-key", default=None)
    parser.add_argument("--secret-key", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--no-https", action="store_true")
    parser.add_argument("--out", default=None, help="Save JSON report to file")
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

    report = generate_report(s3=s3, bucket=args.bucket)
    print_report(report)

    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Report saved to {args.out}")


if __name__ == "__main__":
    main()