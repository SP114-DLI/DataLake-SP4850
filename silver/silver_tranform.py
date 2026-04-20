"""
Silver layer transformation: raw JSONL (bronze) -> clean, deduplicated Parquet (silver).

Guarantees:
  - Schema enforced on write (no drift)
  - All columns snake_case
  - Nested JSON fully flattened
  - Deduplicated by VIN (business key)
  - Invalid / VIN-less records quarantined with reason
  - Nulls handled per-column (coerced, flagged, or dropped)
  - Dates normalized to ISO 8601 UTC
  - Metadata on every record: source, ingestion ts, processing ts, run id
  - Idempotent: re-runs with the same data produce the same result, no duplicates
  - Incremental by default; full-refresh available via --full-refresh

Usage:
    python -m silver.silver_transform                   # incremental
    python -m silver.silver_transform --full-refresh    # reprocess everything
"""

import argparse
import json
import logging
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError

from raw.config import (
    ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION, USE_HTTPS,
    BUCKET_RAW, BUCKET_SILVER,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
QUARANTINE_PREFIX = "_quarantine/"
REGISTRY_KEY = "_meta/vin_registry.json"
PIPELINE_SOURCE = "carfax"

# Business key
BUSINESS_KEY = "vin"

# ---------------------------------------------------------------------------
# Silver schema: (silver_column, pandas_dtype)
# Columns not in this map are dropped on write (schema enforcement).
# ---------------------------------------------------------------------------
SILVER_SCHEMA = {
    # identifiers
    "vin":                      "string",
    "listing_id":               "string",
    "stock_number":             "string",
    "vehicle_condition":        "string",   # USED / NEW

    # vehicle attributes
    "year":                     "Int64",
    "make":                     "string",
    "model":                    "string",
    "trim":                     "string",
    "sub_trim":                 "string",
    "bodytype":                 "string",
    "engine":                   "string",
    "displacement":             "string",
    "drivetype":                "string",
    "transmission":             "string",
    "fuel":                     "string",
    "exterior_color":           "string",
    "interior_color":           "string",
    "bed_length":               "string",
    "cab_type":                 "string",
    "mileage":                  "Int64",

    # pricing
    "list_price":               "Float64",
    "current_price":            "Float64",
    "one_price":                "Float64",

    # monthly payment (flattened)
    "monthly_price":            "Float64",
    "monthly_down_pct":         "Float64",
    "monthly_interest_rate":    "Float64",
    "monthly_term_months":      "Int64",
    "monthly_loan_amount":      "Float64",
    "monthly_down_amount":      "Float64",
    "monthly_payment":          "Float64",

    # fuel economy
    "mpg_city":                 "Float64",
    "mpg_highway":              "Float64",
    "mpg_combined":             "Float64",

    # history flags
    "accident_history":         "string",
    "owner_history":            "string",
    "service_history":          "string",
    "service_records":          "Int64",
    "vehicle_use_history":      "string",
    "record_type":              "string",

    # dealer (flattened)
    "dealer_carfax_id":         "string",
    "dealer_name":              "string",
    "dealer_address":           "string",
    "dealer_city":              "string",
    "dealer_state":             "string",
    "dealer_zip":               "string",
    "dealer_phone":             "string",
    "dealer_latitude":          "Float64",
    "dealer_longitude":         "Float64",
    "dealer_avg_rating":        "Float64",
    "dealer_review_count":      "Int64",
    "dealer_type":              "string",
    "dealer_inventory_url":     "string",

    # engagement / scoring
    "image_count":              "Int64",
    "follow_count":             "Int64",
    "distance_to_dealer":       "Float64",
    "tp_retention_score":       "Float64",
    "chiclet_badge":            "string",
    "vdp_url":                  "string",

    # lists stored as JSON strings
    "top_options":              "string",
    "images":                   "string",
    "one_price_arrows":         "string",

    # timestamps (ISO 8601 UTC)
    "first_seen":               "string",

    # lineage metadata
    "_source_system":           "string",
    "_source_file":             "string",
    "_ingestion_ts":            "string",
    "_processing_ts":           "string",
    "_pipeline_run_id":         "string",
}


# ---------------------------------------------------------------------------
# S3 helpers (self-contained — no dependency on s3.py)
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


def _bucket_exists(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False


def _ensure_bucket(s3, bucket):
    if not _bucket_exists(s3, bucket):
        s3.create_bucket(Bucket=bucket)
        logger.info("Created bucket '%s'", bucket)


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


# ---------------------------------------------------------------------------
# VIN registry  (idempotency — tracks VINs already in silver)
# ---------------------------------------------------------------------------

def _load_vin_registry(s3, bucket):
    try:
        body = s3.get_object(Bucket=bucket, Key=REGISTRY_KEY)["Body"].read()
        return set(json.loads(body))
    except (ClientError, json.JSONDecodeError):
        return set()


def _save_vin_registry(s3, bucket, vins: set):
    data = json.dumps(sorted(vins)).encode()
    s3.put_object(Bucket=bucket, Key=REGISTRY_KEY, Body=data)


# ---------------------------------------------------------------------------
# camelCase -> snake_case
# ---------------------------------------------------------------------------

def _to_snake(name):
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


# ---------------------------------------------------------------------------
# Record flattening
# ---------------------------------------------------------------------------

_MONTHLY_MAP = {
    "price":                "monthly_price",
    "downPaymentPercent":   "monthly_down_pct",
    "interestRate":         "monthly_interest_rate",
    "termInMonths":         "monthly_term_months",
    "loanAmount":           "monthly_loan_amount",
    "downPaymentAmount":    "monthly_down_amount",
    "monthlyPayment":       "monthly_payment",
}

_DEALER_MAP = {
    "carfaxId":             "dealer_carfax_id",
    "name":                 "dealer_name",
    "address":              "dealer_address",
    "city":                 "dealer_city",
    "state":                "dealer_state",
    "zip":                  "dealer_zip",
    "phone":                "dealer_phone",
    "latitude":             "dealer_latitude",
    "longitude":            "dealer_longitude",
    "dealerAverageRating":  "dealer_avg_rating",
    "dealerReviewCount":    "dealer_review_count",
    "dealerType":           "dealer_type",
    "dealerInventoryUrl":   "dealer_inventory_url",
}


def _flatten(record):
    """Flatten a single raw JSON record into a dict matching SILVER_SCHEMA keys."""
    flat = {}

    # --- dealer (nested dict) ---
    dealer = record.get("dealer") or {}
    for src, dest in _DEALER_MAP.items():
        flat[dest] = dealer.get(src)

    # --- monthlyPaymentEstimate (nested dict) ---
    monthly = record.get("monthlyPaymentEstimate") or {}
    for src, dest in _MONTHLY_MAP.items():
        flat[dest] = monthly.get(src)

    # --- lists -> JSON strings ---
    for list_field in ("topOptions", "images", "onePriceArrows"):
        val = record.get(list_field)
        flat[_to_snake(list_field)] = json.dumps(val) if val is not None else None

    # --- rename id -> listing_id to avoid collision with Python built-in ---
    flat["listing_id"] = record.get("id")

    # --- remaining scalar fields ---
    _SKIP = {"dealer", "monthlyPaymentEstimate", "topOptions", "images",
             "onePriceArrows", "id",
             # drop redundant atom* / scoring fields not in schema
             "atomMake", "atomModel", "atomTrim", "atomTopOptions",
             "atomOtherOptions", "mediaScores", "baseScore", "sortScore",
             "sortTPScore", "showHbvChange", "advantage",
             "dealerBadgingExperience", "onePriceArrows"}
    for key, value in record.items():
        if key in _SKIP:
            continue
        snake = _to_snake(key)
        if isinstance(value, (list, dict)):
            flat[snake] = json.dumps(value)
        else:
            flat[snake] = value

    return flat


# ---------------------------------------------------------------------------
# Standardisation helpers
# ---------------------------------------------------------------------------

def _standardise_dates(df):
    """Convert date-like string columns to ISO 8601 UTC."""
    if "first_seen" in df.columns:
        parsed = pd.to_datetime(df["first_seen"], errors="coerce", utc=True)
        df["first_seen"] = parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


def _standardise_strings(df):
    """Upper-case make/model for consistency; strip whitespace."""
    for col in ("make", "model", "trim", "sub_trim", "exterior_color",
                "interior_color", "bodytype", "fuel", "drivetype",
                "transmission", "dealer_state", "dealer_city"):
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip().str.upper()
    if "vehicle_condition" in df.columns:
        df["vehicle_condition"] = df["vehicle_condition"].astype("string").str.upper()
    if "dealer_zip" in df.columns:
        df["dealer_zip"] = df["dealer_zip"].astype("string").str.strip().str[:5]
    return df


def _coerce_coords(df):
    """Dealer lat/lon arrive as strings — coerce to float."""
    for col in ("dealer_latitude", "dealer_longitude"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Schema enforcement
# ---------------------------------------------------------------------------

def _enforce_schema(df):
    """Keep only SILVER_SCHEMA columns, cast to declared types."""
    # Add any missing schema columns as NA
    for col, dtype in SILVER_SCHEMA.items():
        if col not in df.columns:
            df[col] = pd.NA

    # Drop columns not in schema
    df = df[[c for c in SILVER_SCHEMA if c in df.columns]]

    # Cast types
    for col, dtype in SILVER_SCHEMA.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError):
                df[col] = pd.to_numeric(df[col], errors="coerce") if "int" in dtype.lower() or "float" in dtype.lower() else df[col].astype("string")

    return df


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------

def _quarantine_invalid(df):
    """
    Separate records that fail validation into a quarantine DataFrame.

    Returns (valid_df, quarantine_df).  quarantine_df has an extra
    '_rejection_reason' column.
    """
    reasons = pd.Series("", index=df.index, dtype="string")

    # Must have a VIN
    no_vin = df[BUSINESS_KEY].isna() | (df[BUSINESS_KEY].str.strip() == "")
    reasons = reasons.where(~no_vin, reasons + "missing_vin;")

    # Year must be a plausible integer
    if "year" in df.columns:
        bad_year = df["year"].isna() | (df["year"] < 1900) | (df["year"] > 2030)
        reasons = reasons.where(~bad_year, reasons + "invalid_year;")

    # Mileage must be non-negative when present
    if "mileage" in df.columns:
        bad_miles = df["mileage"].notna() & (df["mileage"] < 0)
        reasons = reasons.where(~bad_miles, reasons + "negative_mileage;")

    has_reason = reasons.str.len() > 0
    quarantine = df[has_reason].copy()
    quarantine["_rejection_reason"] = reasons[has_reason]

    valid = df[~has_reason].copy()
    return valid, quarantine


# ---------------------------------------------------------------------------
# Core transform pipeline
# ---------------------------------------------------------------------------

def transform_records(records, source_file, run_id, now_utc):
    """
    Full transform pipeline on a list of raw dicts.

    Returns (silver_df, quarantine_df).
    """
    if not records:
        empty = pd.DataFrame(columns=list(SILVER_SCHEMA.keys()))
        return empty, empty.copy()

    # 1. Flatten
    flat = [_flatten(r) for r in records]
    df = pd.DataFrame(flat)

    # 2. Standardise
    df = _standardise_strings(df)
    df = _standardise_dates(df)
    df = _coerce_coords(df)

    # 3. Add lineage metadata
    df["_source_system"] = PIPELINE_SOURCE
    df["_source_file"] = source_file
    df["_ingestion_ts"] = now_utc
    df["_processing_ts"] = now_utc
    df["_pipeline_run_id"] = run_id

    # 4. Enforce schema (drops extra columns, casts types)
    df = _enforce_schema(df)

    # 5. Quarantine invalid records
    valid, quarantine = _quarantine_invalid(df)

    return valid, quarantine


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------

def _upload_parquet(s3, bucket, key, df):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    df.to_parquet(tmp_path, engine="pyarrow", index=False)
    size_mb = tmp_path.stat().st_size / (1024 ** 2)
    s3.upload_file(str(tmp_path), bucket, key)
    tmp_path.unlink()
    return size_mb


def _upload_quarantine(s3, bucket, key, df):
    if df.empty:
        return
    q_key = f"{QUARANTINE_PREFIX}{key}"
    _upload_parquet(s3, bucket, q_key, df)
    logger.info("  Quarantined %d records -> %s", len(df), q_key)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def transform_all(s3=None, raw_bucket=None, silver_bucket=None,
                  full_refresh=False):
    """
    Download raw JSONL, transform, deduplicate, upload Parquet to silver.

    Idempotent: uses a VIN registry so re-runs never duplicate data.
    """
    s3 = s3 or _get_s3()
    raw_bucket = raw_bucket or BUCKET_RAW
    silver_bucket = silver_bucket or BUCKET_SILVER

    if not _bucket_exists(s3, raw_bucket):
        logger.error("Raw bucket '%s' does not exist", raw_bucket)
        return {"success": False, "error": f"Bucket '{raw_bucket}' not found"}

    _ensure_bucket(s3, silver_bucket)

    run_id = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Load VIN registry for cross-file dedup (skip in full-refresh)
    if full_refresh:
        known_vins = set()
        # Clear existing silver parquet (not quarantine/meta) for atomic rewrite
        for key in _list_keys(s3, silver_bucket, suffix=".parquet"):
            if not key.startswith(QUARANTINE_PREFIX) and not key.startswith("_meta/"):
                s3.delete_object(Bucket=silver_bucket, Key=key)
        logger.info("Full refresh: cleared existing silver parquet files")
    else:
        known_vins = _load_vin_registry(s3, silver_bucket)
        logger.info("VIN registry loaded: %d known VINs", len(known_vins))

    # Determine which raw files to process
    jsonl_keys = _list_keys(s3, raw_bucket, suffix=".jsonl")
    if not jsonl_keys:
        logger.warning("No JSONL files in raw bucket")
        return {"success": True, "files_processed": 0}

    existing_parquet = set(_list_keys(s3, silver_bucket, suffix=".parquet"))

    stats = {
        "success": True,
        "run_id": run_id,
        "files_processed": 0,
        "files_skipped": 0,
        "files_failed": 0,
        "total_raw_records": 0,
        "total_silver_records": 0,
        "total_quarantined": 0,
        "total_cross_dedup": 0,
        "files": [],
    }

    for jsonl_key in jsonl_keys:
        parquet_key = jsonl_key.replace(".jsonl", ".parquet")

        # In incremental mode, skip files whose parquet already exists
        if not full_refresh and parquet_key in existing_parquet:
            logger.info("Skipping (already processed): %s", jsonl_key)
            stats["files_skipped"] += 1
            continue

        logger.info("Processing: %s", jsonl_key)
        try:
            # Download
            body = s3.get_object(Bucket=raw_bucket, Key=jsonl_key)["Body"].read().decode("utf-8")
            records = [json.loads(line) for line in body.splitlines() if line.strip()]
            logger.info("  Downloaded %d records", len(records))

            # Transform
            silver_df, quarantine_df = transform_records(records, jsonl_key, run_id, now_utc)

            # Cross-file VIN dedup against registry
            if not silver_df.empty and known_vins:
                before = len(silver_df)
                silver_df = silver_df[~silver_df[BUSINESS_KEY].isin(known_vins)]
                cross_dupes = before - len(silver_df)
                if cross_dupes:
                    logger.info("  Removed %d cross-file duplicate VINs", cross_dupes)
                    stats["total_cross_dedup"] += cross_dupes

            # Upload quarantine
            _upload_quarantine(s3, silver_bucket, parquet_key, quarantine_df)

            # Upload silver
            if silver_df.empty:
                logger.info("  No new records after dedup, skipping upload")
                stats["files_skipped"] += 1
                continue

            size_mb = _upload_parquet(s3, silver_bucket, parquet_key, silver_df)
            logger.info("  Uploaded %s (%.2f MB, %d rows)", parquet_key, size_mb, len(silver_df))

            # Update registry
            known_vins.update(silver_df[BUSINESS_KEY].tolist())

            stats["files_processed"] += 1
            stats["total_raw_records"] += len(records)
            stats["total_silver_records"] += len(silver_df)
            stats["total_quarantined"] += len(quarantine_df)
            stats["files"].append({
                "source": jsonl_key,
                "dest": parquet_key,
                "raw": len(records),
                "silver": len(silver_df),
                "quarantined": len(quarantine_df),
                "size_mb": round(size_mb, 2),
            })

        except Exception as e:
            logger.error("  Failed: %s: %s", jsonl_key, e, exc_info=True)
            stats["files_failed"] += 1
            stats["files"].append({"source": jsonl_key, "error": str(e)})

    # Persist VIN registry
    _save_vin_registry(s3, silver_bucket, known_vins)
    logger.info("VIN registry saved: %d total VINs", len(known_vins))

    stats["success"] = stats["files_failed"] == 0
    logger.info(
        "Done — processed: %d, skipped: %d, failed: %d | "
        "raw: %d -> silver: %d, quarantined: %d, cross-dedup: %d",
        stats["files_processed"], stats["files_skipped"], stats["files_failed"],
        stats["total_raw_records"], stats["total_silver_records"],
        stats["total_quarantined"], stats["total_cross_dedup"],
    )
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Silver layer transform")
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--access-key", default=None)
    parser.add_argument("--secret-key", default=None)
    parser.add_argument("--raw-bucket", default=None)
    parser.add_argument("--silver-bucket", default=None)
    parser.add_argument("--full-refresh", action="store_true",
                        help="Reprocess all raw files (clear and rebuild silver)")
    parser.add_argument("--no-https", action="store_true")
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

    result = transform_all(
        s3=s3,
        raw_bucket=args.raw_bucket,
        silver_bucket=args.silver_bucket,
        full_refresh=args.full_refresh,
    )

    out = Path("reports/silver_transform_results.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
    logger.info("Results -> %s", out)

    return 0 if result["success"] else 1


if __name__ == "__main__":
    exit(main())