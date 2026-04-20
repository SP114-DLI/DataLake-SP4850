"""
Silver → Gold Transformation: Car Listings (Carfax / CarfaxMarket)
===================================================================
Reads silver Parquet from MinIO (lakesilver), produces 5 gold tables,
and uploads them to the gold bucket (lakegold).

Gold Tables:
  1. gold_listings_enriched      — row-level fact table with derived metrics
  2. gold_market_summary         — avg/median price & mileage by make/model/year
  3. gold_price_segments         — price tier distribution with top makes
  4. gold_dealer_performance     — dealer benchmarks (price, rating, inventory size)
  5. gold_depreciation_curve     — retained value % by vehicle age

Usage:
    python -m gold.gold_tranform                  # default buckets
    python -m gold.gold_tranform --full-refresh   # clear gold and rebuild

Dependencies:
    pip install pandas pyarrow boto3 numpy
"""

import argparse
import json
import logging
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from raw.config import (
    ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION, USE_HTTPS,
    BUCKET_SILVER, BUCKET_GOLD,
)

logger = logging.getLogger(__name__)

CURRENT_YEAR = 2026

# ---------------------------------------------------------------------------
# PRICE SEGMENT BUCKETS
# ---------------------------------------------------------------------------
PRICE_BINS   = [0, 10_000, 20_000, 35_000, 60_000, 100_000, float("inf")]
PRICE_LABELS = ["Budget", "Economy", "Mid-Range", "Premium", "Luxury", "Ultra-Luxury"]

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


def _download_parquet(s3, bucket, key):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    s3.download_file(bucket, key, str(tmp_path))
    df = pd.read_parquet(tmp_path)
    tmp_path.unlink()
    return df


def _upload_parquet(s3, bucket, key, df):
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    df.to_parquet(tmp_path, engine="pyarrow", index=False)
    size_mb = tmp_path.stat().st_size / (1024 ** 2)
    s3.upload_file(str(tmp_path), bucket, key)
    tmp_path.unlink()
    return size_mb


# ---------------------------------------------------------------------------
# HELPERS — parse JSON columns safely
# ---------------------------------------------------------------------------

def extract_accident_flag(val) -> bool:
    """Returns True if an accident has been reported."""
    try:
        d = json.loads(val) if isinstance(val, str) else val
        return "accident" in str(d.get("icon", "")).lower()
    except Exception:
        return False


def extract_owner_count(val) -> int | None:
    """Returns the number of previous owners from the owner_history JSON blob."""
    try:
        d = json.loads(val) if isinstance(val, str) else val
        history = d.get("history", [])
        return len(history)
    except Exception:
        return None


def extract_service_record_count(val) -> int | None:
    """Returns total number of service events from service_history JSON."""
    try:
        d = json.loads(val) if isinstance(val, str) else val
        return int(d.get("number", 0)) or None
    except Exception:
        return None


def extract_top_options(val) -> list:
    """Parse top_options JSON array to a Python list."""
    try:
        if isinstance(val, str):
            return json.loads(val)
        return val or []
    except Exception:
        return []


# ---------------------------------------------------------------------------
# LOAD & CLEAN
# ---------------------------------------------------------------------------

def load_silver(s3, bucket):
    """Download and concatenate all silver Parquet files from the bucket."""
    silver_keys = [
        k for k in _list_keys(s3, bucket, suffix=".parquet")
        if not k.startswith(QUARANTINE_PREFIX) and not k.startswith("_meta/")
    ]

    if not silver_keys:
        logger.warning("No silver Parquet files found in '%s'", bucket)
        return pd.DataFrame()

    frames = []
    for key in silver_keys:
        logger.info("Reading %s", key)
        try:
            frames.append(_download_parquet(s3, bucket, key))
        except Exception as e:
            logger.error("Failed to read %s: %s", key, e)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d rows x %d columns from %d silver files",
                len(df), len(df.columns), len(frames))
    return df


def clean_and_cast(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning and casting...")

    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["list_price"]    = pd.to_numeric(df["list_price"],    errors="coerce")
    df["mileage"]       = pd.to_numeric(df["mileage"],       errors="coerce")
    df["year"]          = pd.to_numeric(df["year"],          errors="coerce").astype("Int64")

    df = df[df["current_price"] > 0]
    df = df[df["year"].between(1980, CURRENT_YEAR + 1)]
    df = df[df["mileage"] >= 0]

    for col in ["make", "model", "trim", "bodytype", "fuel", "vehicle_condition",
                "dealer_type", "drivetype", "transmission", "exterior_color"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    if "first_seen" in df.columns:
        df["first_seen"] = pd.to_datetime(df["first_seen"], errors="coerce", utc=True)

    logger.info("%d rows after cleaning", len(df))
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# GOLD TABLE 1 — Enriched Listings Fact Table
# ---------------------------------------------------------------------------

def build_gold_listings_enriched(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building gold_listings_enriched...")
    g = df.copy()

    g["has_accident"]        = g["accident_history"].apply(extract_accident_flag)
    g["owner_count"]         = g["owner_history"].apply(extract_owner_count)
    g["service_event_count"] = g["service_history"].apply(extract_service_record_count)
    g["option_count"]        = g["top_options"].apply(lambda x: len(extract_top_options(x)))

    g["vehicle_age_years"] = CURRENT_YEAR - g["year"]

    g["price_discount_abs"] = (g["list_price"] - g["current_price"]).round(2)
    g["price_discount_pct"] = np.where(
        g["list_price"] > 0,
        ((g["price_discount_abs"] / g["list_price"]) * 100).round(2),
        np.nan
    )

    g["price_segment"] = pd.cut(
        g["current_price"], bins=PRICE_BINS, labels=PRICE_LABELS, right=False
    )

    g["mileage_band"] = pd.cut(
        g["mileage"],
        bins=[0, 20_000, 50_000, 100_000, 150_000, float("inf")],
        labels=["Very Low (<20k)", "Low (20-50k)", "Mid (50-100k)",
                "High (100-150k)", "Very High (150k+)"],
        right=False
    )

    g["cost_per_mile"] = np.where(
        g["mileage"] > 0,
        (g["current_price"] / g["mileage"]).round(4),
        np.nan
    )

    if "first_seen" in g.columns:
        g["days_on_market"] = (
            pd.Timestamp.now(tz="UTC") - g["first_seen"]
        ).dt.days.clip(lower=0)

    g["segment_median_price"] = g.groupby(
        ["make", "model", "year"]
    )["current_price"].transform("median")

    g["price_vs_market"] = np.select(
        [
            g["current_price"] < g["segment_median_price"] * 0.90,
            g["current_price"] > g["segment_median_price"] * 1.10,
        ],
        ["Below Market", "Above Market"],
        default="At Market"
    )

    age_score     = np.clip(1 - g["vehicle_age_years"] / 20, 0, 1)
    mile_score    = np.clip(1 - g["mileage"] / 200_000, 0, 1)
    accident_pen  = np.where(g["has_accident"], 0.0, 1.0)
    service_score = np.clip(g["service_event_count"].fillna(0) / 20, 0, 1)

    g["quality_score"] = (
        age_score     * 0.25 +
        mile_score    * 0.35 +
        accident_pen  * 0.25 +
        service_score * 0.15
    ).round(3)

    g["monthly_pmt_pct_of_price"] = np.where(
        (g["current_price"] > 0) & (g["monthly_term_months"] > 0),
        (g["monthly_payment"] / g["current_price"] * 100).round(3),
        np.nan
    )

    drop_cols = [
        "accident_history", "owner_history", "service_history",
        "vehicle_use_history", "one_price_arrows", "images",
        "_source_system", "_source_file", "_ingestion_ts",
        "_processing_ts", "_pipeline_run_id", "top_options",
    ]
    g = g.drop(columns=[c for c in drop_cols if c in g.columns])

    logger.info("gold_listings_enriched: %d rows, %d columns", len(g), len(g.columns))
    return g


# ---------------------------------------------------------------------------
# GOLD TABLE 2 — Market Summary by Make / Model / Year / Body Type
# ---------------------------------------------------------------------------

def build_gold_market_summary(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building gold_market_summary...")

    agg = df.groupby(["make", "model", "year", "bodytype"]).agg(
        listing_count       = ("current_price", "count"),
        price_min           = ("current_price", "min"),
        price_max           = ("current_price", "max"),
        price_mean          = ("current_price", "mean"),
        price_median        = ("current_price", "median"),
        price_std           = ("current_price", "std"),
        list_price_mean     = ("list_price", "mean"),
        avg_mileage         = ("mileage", "mean"),
        median_mileage      = ("mileage", "median"),
        avg_mpg_city        = ("mpg_city", "mean"),
        avg_mpg_highway     = ("mpg_highway", "mean"),
        avg_mpg_combined    = ("mpg_combined", "mean"),
        avg_dealer_rating   = ("dealer_avg_rating", "mean"),
        avg_follow_count    = ("follow_count", "mean"),
        avg_retention_score = ("tp_retention_score", "mean"),
    ).round(2).reset_index()

    agg["price_spread"]     = (agg["price_max"] - agg["price_min"]).round(2)
    agg["price_cv_pct"]     = (agg["price_std"] / agg["price_mean"] * 100).round(2)
    agg["avg_discount_pct"] = (
        (agg["list_price_mean"] - agg["price_mean"]) / agg["list_price_mean"] * 100
    ).round(2)

    logger.info("gold_market_summary: %d rows", len(agg))
    return agg


# ---------------------------------------------------------------------------
# GOLD TABLE 3 — Price Segment Distribution
# ---------------------------------------------------------------------------

def build_gold_price_segments(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building gold_price_segments...")

    df = df.copy()
    df["price_segment"] = pd.cut(
        df["current_price"], bins=PRICE_BINS, labels=PRICE_LABELS, right=False
    )

    total = len(df)
    seg = (
        df.groupby("price_segment", observed=True)
        .agg(
            listing_count       = ("current_price", "count"),
            avg_price           = ("current_price", "mean"),
            median_price        = ("current_price", "median"),
            min_price           = ("current_price", "min"),
            max_price           = ("current_price", "max"),
            avg_mileage         = ("mileage", "mean"),
            avg_vehicle_age     = ("year", lambda x: (CURRENT_YEAR - x).mean()),
            avg_retention_score = ("tp_retention_score", "mean"),
            avg_mpg_combined    = ("mpg_combined", "mean"),
        )
        .round(2)
        .reset_index()
    )
    seg["pct_of_market"] = (seg["listing_count"] / total * 100).round(2)

    top_makes = (
        df.groupby(["price_segment", "make"], observed=True)
        .size().reset_index(name="n")
        .sort_values(["price_segment", "n"], ascending=[True, False])
        .groupby("price_segment", observed=True).head(3)
        .groupby("price_segment", observed=True)["make"]
        .apply(lambda x: ", ".join(x))
        .reset_index(name="top_makes")
    )
    seg = seg.merge(top_makes, on="price_segment", how="left")

    top_body = (
        df.groupby(["price_segment", "bodytype"], observed=True)
        .size().reset_index(name="n")
        .sort_values(["price_segment", "n"], ascending=[True, False])
        .groupby("price_segment", observed=True).first()
        .reset_index()[["price_segment", "bodytype"]]
        .rename(columns={"bodytype": "dominant_body_style"})
    )
    seg = seg.merge(top_body, on="price_segment", how="left")

    logger.info("gold_price_segments: %d rows", len(seg))
    return seg


# ---------------------------------------------------------------------------
# GOLD TABLE 4 — Dealer Performance Benchmarks
# ---------------------------------------------------------------------------

def build_gold_dealer_performance(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building gold_dealer_performance...")

    dealer_agg = df.groupby(
        ["dealer_carfax_id", "dealer_name", "dealer_city",
         "dealer_state", "dealer_type", "dealer_avg_rating", "dealer_review_count"]
    ).agg(
        inventory_count     = ("listing_id", "count"),
        avg_price           = ("current_price", "mean"),
        median_price        = ("current_price", "median"),
        avg_list_price      = ("list_price", "mean"),
        avg_mileage         = ("mileage", "mean"),
        avg_image_count     = ("image_count", "mean"),
        avg_follow_count    = ("follow_count", "mean"),
        avg_retention_score = ("tp_retention_score", "mean"),
        pct_accident        = ("accident_history",
                               lambda x: round(x.apply(extract_accident_flag).mean() * 100, 2)),
        avg_vehicle_age     = ("year", lambda x: round((CURRENT_YEAR - x).mean(), 1)),
        makes_offered       = ("make", lambda x: ", ".join(sorted(x.unique()))),
    ).round(2).reset_index()

    dealer_agg["avg_discount_pct"] = (
        (dealer_agg["avg_list_price"] - dealer_agg["avg_price"])
        / dealer_agg["avg_list_price"] * 100
    ).round(2)

    dealer_agg["inventory_tier"] = pd.cut(
        dealer_agg["inventory_count"],
        bins=[0, 10, 50, 150, float("inf")],
        labels=["Small (<10)", "Mid (10-50)", "Large (50-150)", "Super (150+)"]
    )

    logger.info("gold_dealer_performance: %d rows", len(dealer_agg))
    return dealer_agg


# ---------------------------------------------------------------------------
# GOLD TABLE 5 — Depreciation Curve by Make & Vehicle Age
# ---------------------------------------------------------------------------

def build_gold_depreciation_curve(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Building gold_depreciation_curve...")

    df = df.copy()
    df["vehicle_age"] = CURRENT_YEAR - df["year"]
    df = df[df["vehicle_age"].between(0, 25)]

    curve = (
        df.groupby(["make", "vehicle_age"])
        .agg(
            listing_count = ("current_price", "count"),
            avg_price     = ("current_price", "mean"),
            median_price  = ("current_price", "median"),
            avg_mileage   = ("mileage", "mean"),
        )
        .round(2)
        .reset_index()
    )

    baseline = (
        curve[curve["vehicle_age"] <= 1]
        .groupby("make")["avg_price"].mean()
        .rename("baseline_price")
    )
    curve = curve.merge(baseline, on="make", how="left")
    curve["retained_value_pct"] = (
        curve["avg_price"] / curve["baseline_price"] * 100
    ).round(2)

    curve = curve.sort_values(["make", "vehicle_age"])
    curve["next_yr_avg_price"]    = curve.groupby("make")["avg_price"].shift(-1)
    curve["yoy_depreciation_pct"] = (
        (curve["avg_price"] - curve["next_yr_avg_price"]) / curve["avg_price"] * 100
    ).round(2)

    logger.info("gold_depreciation_curve: %d rows", len(curve))
    return curve


# ---------------------------------------------------------------------------
# SAVE TO S3
# ---------------------------------------------------------------------------

def save_gold(s3, bucket, tables: dict, run_id: str):
    """Upload gold tables as Parquet files to the gold S3 bucket."""
    _ensure_bucket(s3, bucket)
    logger.info("Saving gold tables to bucket '%s'", bucket)

    for name, df in tables.items():
        if df.empty:
            logger.warning("SKIPPED (empty): %s", name)
            continue
        key = f"{name}.parquet"
        size_mb = _upload_parquet(s3, bucket, key, df)
        logger.info("Saved %s (%d rows, %d cols, %.2f MB)",
                    key, len(df), len(df.columns), size_mb)

    # Save run metadata
    meta = {
        "run_id": run_id,
        "completed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tables": {
            name: {"rows": len(df), "columns": len(df.columns)}
            for name, df in tables.items()
        },
    }
    s3.put_object(
        Bucket=bucket,
        Key="_meta/last_run.json",
        Body=json.dumps(meta, indent=2).encode(),
    )
    logger.info("Run metadata saved to _meta/last_run.json")


# ---------------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------------

def transform_all(s3=None, silver_bucket=None, gold_bucket=None,
                  full_refresh=False):
    """
    Download silver Parquet, build gold tables, upload to gold bucket.
    """
    s3 = s3 or _get_s3()
    silver_bucket = silver_bucket or BUCKET_SILVER
    gold_bucket = gold_bucket or BUCKET_GOLD
    run_id = str(uuid.uuid4())

    if not _bucket_exists(s3, silver_bucket):
        logger.error("Silver bucket '%s' does not exist", silver_bucket)
        return {"success": False, "error": f"Bucket '{silver_bucket}' not found"}

    if full_refresh:
        _ensure_bucket(s3, gold_bucket)
        for key in _list_keys(s3, gold_bucket, suffix=".parquet"):
            s3.delete_object(Bucket=gold_bucket, Key=key)
        logger.info("Full refresh: cleared existing gold parquet files")

    # Load silver data
    df = load_silver(s3, silver_bucket)
    if df.empty:
        return {"success": False, "error": "No silver data to transform"}

    df = clean_and_cast(df)
    if df.empty:
        return {"success": False, "error": "No rows survived cleaning"}

    # Build gold tables
    gold_tables = {
        "gold_listings_enriched":  build_gold_listings_enriched(df),
        "gold_market_summary":     build_gold_market_summary(df),
        "gold_price_segments":     build_gold_price_segments(df),
        "gold_dealer_performance": build_gold_dealer_performance(df),
        "gold_depreciation_curve": build_gold_depreciation_curve(df),
    }

    # Upload to S3
    save_gold(s3, gold_bucket, gold_tables, run_id)

    result = {
        "success": True,
        "run_id": run_id,
        "silver_rows_loaded": len(df),
        "tables": {
            name: {"rows": len(tbl), "columns": len(tbl.columns)}
            for name, tbl in gold_tables.items()
        },
    }

    logger.info("Gold layer complete.")
    for name, tbl in gold_tables.items():
        status = f"{len(tbl):,} rows" if not tbl.empty else "SKIPPED"
        logger.info("  %-35s %s", name, status)

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Silver to Gold: Car Listings")
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--access-key", default=None)
    parser.add_argument("--secret-key", default=None)
    parser.add_argument("--silver-bucket", default=None)
    parser.add_argument("--gold-bucket", default=None)
    parser.add_argument("--full-refresh", action="store_true",
                        help="Clear gold bucket and rebuild all tables")
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
        silver_bucket=args.silver_bucket,
        gold_bucket=args.gold_bucket,
        full_refresh=args.full_refresh,
    )

    out = Path("reports/gold_transform_results.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(result, f, indent=2)
    logger.info("Results -> %s", out)

    return 0 if result["success"] else 1


if __name__ == "__main__":
    exit(main())
