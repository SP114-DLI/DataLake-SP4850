"""
Raw (Bronze) layer data description.

Reads JSONL files from the raw bucket and prints a summarized description:
shape, top-level key inventory, sample value types, and basic stats.

Usage:
    python -m raw.raw_description
    python -m raw.raw_description --out raw_description.json
"""

import argparse
import json
import logging
from collections import Counter

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from raw.config import (
    ENDPOINT, ACCESS_KEY, SECRET_KEY, REGION, USE_HTTPS,
    BUCKET_RAW,
)

logger = logging.getLogger(__name__)


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


def _list_keys(s3, bucket, suffix=""):
    keys = []
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(suffix):
                    keys.append({"key": obj["Key"], "size": obj["Size"]})
    except ClientError:
        pass
    return sorted(keys, key=lambda x: x["key"])


# ---------------------------------------------------------------------------
# Description
# ---------------------------------------------------------------------------

def describe_raw(s3=None, bucket=None, sample_bytes=2 * 1024 * 1024):
    """
    Describe the raw layer by reading a small byte-range sample from each
    JSONL file (default 2 MB head) instead of downloading the full file.

    Returns a summary dict with file inventory, estimated record counts,
    key frequency, type breakdown, and sample values.
    """
    s3 = s3 or _get_s3()
    bucket = bucket or BUCKET_RAW

    file_info = _list_keys(s3, bucket, suffix=".jsonl")
    if not file_info:
        return {"error": "No JSONL files found in raw bucket"}

    total_records_est = 0
    key_counter = Counter()
    type_counter = {}  # key -> Counter of types
    nested_keys = set()
    sample_values = {}  # key -> first non-null value seen
    files_summary = []

    for fi in file_info:
        key = fi["key"]
        file_size = fi["size"]
        size_mb = round(file_size / (1024 ** 2), 2)
        logger.info("Sampling %s (%.1f MB)", key, size_mb)

        try:
            # Read only the first sample_bytes of the file
            range_end = min(sample_bytes, file_size) - 1
            resp = s3.get_object(
                Bucket=bucket, Key=key,
                Range=f"bytes=0-{range_end}",
            )
            chunk = resp["Body"].read().decode("utf-8")
            # Only use complete lines (last line may be truncated)
            lines = chunk.split("\n")
            if not chunk.endswith("\n"):
                lines = lines[:-1]
            lines = [l for l in lines if l.strip()]

            sampled = len(lines)
            # Estimate total records from avg line size
            if sampled > 0:
                avg_line = len(chunk.encode()) / sampled
                est_records = int(file_size / avg_line)
            else:
                est_records = 0
            total_records_est += est_records
            files_summary.append({
                "file": key, "size_mb": size_mb,
                "sampled_records": sampled,
                "est_total_records": est_records,
            })

            for line in lines:
                record = json.loads(line)
                for k, v in record.items():
                    key_counter[k] += 1
                    if k not in type_counter:
                        type_counter[k] = Counter()
                    vtype = type(v).__name__
                    if isinstance(v, dict):
                        nested_keys.add(k)
                        vtype = f"dict({len(v)} keys)"
                    elif isinstance(v, list):
                        vtype = f"list(len={len(v)})"
                    type_counter[k][vtype] += 1

                    if k not in sample_values and v is not None:
                        if isinstance(v, (dict, list)):
                            sample_values[k] = json.dumps(v)[:120]
                        else:
                            sample_values[k] = str(v)[:120]

        except Exception as e:
            logger.error("Failed to read %s: %s", key, e)
            files_summary.append({"file": key, "size_mb": size_mb, "error": str(e)})

    # Build key info sorted by frequency
    keys_desc = {}
    for k in sorted(key_counter, key=lambda x: -key_counter[x]):
        keys_desc[k] = {
            "seen_in_samples": key_counter[k],
            "types": dict(type_counter.get(k, {}).most_common()),
            "nested": k in nested_keys,
            "sample": sample_values.get(k),
        }

    total_size_mb = round(sum(f["size_mb"] for f in files_summary), 2)

    return {
        "total_files": len(file_info),
        "total_records": total_records_est,
        "total_size_mb": total_size_mb,
        "files": files_summary,
        "top_level_keys": len(keys_desc),
        "nested_keys": sorted(nested_keys),
        "keys": keys_desc,
    }


# ---------------------------------------------------------------------------
# Pretty-print
# ---------------------------------------------------------------------------

def print_description(desc):
    print("=" * 70)
    print("  RAW (BRONZE) LAYER DATA DESCRIPTION")
    print(f"  Files: {desc.get('total_files', 0)}  |  "
          f"Records (est): {desc.get('total_records', 0):,}  |  "
          f"Size: {desc.get('total_size_mb', 0):.1f} MB")
    print("=" * 70)

    # File inventory
    print("\n  FILES")
    print(f"  {'File':<40} {'Size MB':>8} {'Sampled':>8} {'Est Total':>10}")
    print("  " + "-" * 70)
    for f in desc.get("files", []):
        if "error" in f:
            print(f"  {f['file']:<40} {f['size_mb']:>8.1f}   ERROR: {f['error']}")
        else:
            print(f"  {f['file']:<40} {f['size_mb']:>8.1f} "
                  f"{f.get('sampled_records', 0):>8,} "
                  f"{f.get('est_total_records', 0):>10,}")

    # Keys
    keys = desc.get("keys", {})
    print(f"\n  TOP-LEVEL KEYS ({len(keys)} total, "
          f"{len(desc.get('nested_keys', []))} nested)")
    print(f"  {'Key':<35} {'Samples':>8} {'Type':<25} {'Nested':>6}")
    print("  " + "-" * 78)
    for k, info in keys.items():
        types = info.get("types", {})
        main_type = next(iter(types), "?")
        nested = "Yes" if info.get("nested") else ""
        print(f"  {k:<35} {info['seen_in_samples']:>8} {main_type:<25} {nested:>6}")

    # Nested key details
    nested = desc.get("nested_keys", [])
    if nested:
        print(f"\n  NESTED OBJECTS: {', '.join(nested)}")

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Raw layer data description")
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

    desc = describe_raw(s3=s3, bucket=args.bucket)
    print_description(desc)

    if args.out:
        with open(args.out, "w") as f:
            json.dump(desc, f, indent=2)
        print(f"Description saved to {args.out}")


if __name__ == "__main__":
    main()
