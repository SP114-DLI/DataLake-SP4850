"""
Carfax vehicle data pipeline: scrape -> collect JSONL (~512MB files) -> upload to MinIO.

Usage:
    python -m raw.pipeline scrape   --zip-file zip_codes.txt --condition USED
    python -m raw.pipeline upload   --data-dir data
    python -m raw.pipeline full     --zip-file zip_codes.txt --condition USED
"""

import argparse
import hashlib
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from minio import Minio
from minio.error import S3Error

# ---------------------------------------------------------------------------
# User config (cookies / headers live in project-root user_config.py)
# ---------------------------------------------------------------------------
try:
    from user_config import COOKIES, HEADERS
except ImportError:
    COOKIES, HEADERS = {}, {}

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
from raw.config import ENDPOINT, ACCESS_KEY, SECRET_KEY, USE_HTTPS, BUCKET_RAW

# Carfax API
BASE_URL = "https://helix.carfax.com/search/v2/vehicles"
MAX_RETRIES = 6
INITIAL_WAIT = 5  # seconds

# File storage
FILE_SIZE_LIMIT = 512 * 1024 * 1024  # 512 MB
MANIFEST_SAVE_INTERVAL = 10  # rows between periodic manifest saves

logger = logging.getLogger(__name__)


# ===========================================================================
# Progress tracking  (by zip code + year)
# ===========================================================================

def _load_progress(progress_file):
    if os.path.exists(progress_file):
        try:
            with open(progress_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def _save_progress(progress_dict, progress_file):
    Path(progress_file).parent.mkdir(parents=True, exist_ok=True)
    with open(progress_file, "w") as f:
        json.dump(progress_dict, f, indent=2)


def mark_completed(key, progress_file, count=0, error=None):
    progress = _load_progress(progress_file)
    progress[key] = {
        "completed_at": datetime.now().isoformat(),
        "count": count,
        "error": error,
    }
    _save_progress(progress, progress_file)


def is_processed(key, progress_file):
    return key in _load_progress(progress_file)


def get_processed_keys(progress_file):
    return set(_load_progress(progress_file).keys())


def load_zip_codes(filepath):
    with open(filepath, "r") as f:
        return [line.strip() for line in f if line.strip()]


# ===========================================================================
# Carfax API client
# ===========================================================================

def search_vehicles(zip_code, year_min=None, year_max=None,
                    vehicle_condition="USED", radius=25, rows=25, page=1):
    params = {
        "zip": zip_code,
        "radius": radius,
        "sort": "LOCATION_NEAREST",
        "vehicleCondition": vehicle_condition,
        "rows": rows,
        "dynamicRadius": "false",
        "page": page,
    }
    if year_min is not None:
        params["yearMin"] = year_min
    if year_max is not None:
        params["yearMax"] = year_max

    wait = INITIAL_WAIT
    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.get(BASE_URL, headers=HEADERS, cookies=COOKIES, params=params)
        if response.status_code == 200:
            return response.json()
        logger.warning("HTTP %d (attempt %d/%d). Waiting %ds...",
                       response.status_code, attempt, MAX_RETRIES, wait)
        time.sleep(wait)
        wait *= 2

    response.raise_for_status()


def fetch_all_pages(zip_code, year_min, year_max, vehicle_condition,
                    radius=25, rows=25, delay=1.0):
    """Fetch all listing objects across all pages for a zip/year combo."""
    all_listings = []
    data = search_vehicles(zip_code, year_min, year_max, vehicle_condition,
                           radius, rows, page=1)
    total_pages = data.get("totalPageCount", 1)
    total_count = data.get("totalListingCount", 0)

    logger.info("  Zip %s: %d listings, %d pages", zip_code, total_count, total_pages)
    all_listings.extend(data.get("listings", []))

    for page in range(2, total_pages + 1):
        time.sleep(delay)
        try:
            data = search_vehicles(zip_code, year_min, year_max,
                                   vehicle_condition, radius, rows, page=page)
            all_listings.extend(data.get("listings", []))
        except Exception as e:
            logger.error("    Error on page %d: %s", page, e)

    return all_listings


# ===========================================================================
# JSONL file storage with automatic ~512 MB rotation
# ===========================================================================

class FileStorageManager:
    """Writes listings to JSONL files, rotating at FILE_SIZE_LIMIT."""

    def __init__(self, output_dir, base_filename, manifest_file="file_manifest.json"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.base_filename = base_filename
        self.manifest_file = self.output_dir / manifest_file

        self.current_file_handle = None
        self.current_file_path = None
        self.current_file_size = 0
        self.current_file_row_count = 0
        self.current_part_num = 1
        self.current_file_hash = hashlib.md5()
        self._rows_since_save = 0
        self._seen_vins = set()  # intra-file VIN dedup
        self._dupes_skipped = 0

        self.manifest = self._load_manifest()
        self._recover_state()

    # -- manifest ----------------------------------------------------------

    def _load_manifest(self):
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"files": [], "created_at": datetime.now().isoformat(), "last_updated": None}

    def _save_manifest(self):
        self.manifest["last_updated"] = datetime.now().isoformat()
        with open(self.manifest_file, "w") as f:
            json.dump(self.manifest, f, indent=2)

    # -- state recovery ----------------------------------------------------

    def _recover_state(self):
        if not self.manifest["files"]:
            return
        max_part = max(f["part_number"] for f in self.manifest["files"])
        last = next(f for f in self.manifest["files"] if f["part_number"] == max_part)
        self.current_part_num = max_part
        self.current_file_path = self.output_dir / last["filename"]
        if last["size_bytes"] < FILE_SIZE_LIMIT and self.current_file_path.exists():
            self.current_file_size = last["size_bytes"]
            self.current_file_row_count = last["row_count"]
            self.current_file_handle = open(self.current_file_path, "ab")
        else:
            self.current_file_handle = None

    # -- file operations ---------------------------------------------------

    def _get_filename(self):
        return f"{self.base_filename}_part_{self.current_part_num:03d}.jsonl"

    def _open_new_file(self):
        if self.current_file_handle and not self.current_file_handle.closed:
            self._finalize_current_file()
        self.current_part_num += 1
        self.current_file_path = self.output_dir / self._get_filename()
        self.current_file_handle = open(self.current_file_path, "wb")
        self.current_file_size = 0
        self.current_file_row_count = 0
        self.current_file_hash = hashlib.md5()
        self._seen_vins = set()
        self._dupes_skipped = 0

    def _finalize_current_file(self):
        if not self.current_file_handle or self.current_file_handle.closed:
            return
        if self._dupes_skipped:
            logger.info("  Skipped %d intra-file duplicate VINs in %s",
                        self._dupes_skipped, self.current_file_path.name)
        self.current_file_handle.close()
        self.manifest["files"].append({
            "part_number": self.current_part_num,
            "filename": self.current_file_path.name,
            "filepath": str(self.current_file_path),
            "size_bytes": self.current_file_size,
            "row_count": self.current_file_row_count,
            "md5_hash": self.current_file_hash.hexdigest(),
            "created_at": datetime.now().isoformat(),
            "is_complete": self.current_file_size >= FILE_SIZE_LIMIT,
        })
        self._save_manifest()

    # -- writing -----------------------------------------------------------

    def append(self, listing):
        vin = listing.get("vin")
        if vin and vin in self._seen_vins:
            self._dupes_skipped += 1
            return
        if vin:
            self._seen_vins.add(vin)

        if self.current_file_handle is None:
            self.current_file_path = self.output_dir / self._get_filename()
            self.current_file_handle = open(self.current_file_path, "wb")
            self.current_file_size = 0
            self.current_file_row_count = 0
            self.current_file_hash = hashlib.md5()

        line = json.dumps(listing, separators=(",", ":")).encode("utf-8") + b"\n"
        if self.current_file_size + len(line) > FILE_SIZE_LIMIT and self.current_file_size > 0:
            self._open_new_file()

        self.current_file_handle.write(line)
        self.current_file_size += len(line)
        self.current_file_row_count += 1
        self.current_file_hash.update(line)
        self._rows_since_save += 1

        if self._rows_since_save >= MANIFEST_SAVE_INTERVAL:
            self.current_file_handle.flush()
            self._save_manifest()
            self._rows_since_save = 0

    def append_batch(self, listings):
        for listing in listings:
            self.append(listing)

    def close(self):
        self._finalize_current_file()
        self.current_file_handle = None

    def get_file_info(self):
        if self.current_file_path is None:
            return None
        return {
            "filename": self.current_file_path.name,
            "size_bytes": self.current_file_size,
            "row_count": self.current_file_row_count,
            "size_mb": self.current_file_size / (1024 * 1024),
        }

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ===========================================================================
# MinIO upload  (skip files already present with matching size)
# ===========================================================================

def _get_bucket_objects(client, bucket_name):
    objects = {}
    try:
        for obj in client.list_objects(bucket_name):
            objects[obj.object_name] = {"size": obj.size, "etag": obj.etag}
    except Exception as e:
        logger.warning("Failed to list bucket objects: %s", e)
    return objects


def upload_to_minio(
    data_dir="data",
    manifest_path=None,
    endpoint=None,
    access_key=None,
    secret_key=None,
    bucket_name=None,
    use_https=None,
):
    """Upload JSONL files + manifest to MinIO, skipping those already present."""
    endpoint = endpoint or ENDPOINT
    access_key = access_key or ACCESS_KEY
    secret_key = secret_key or SECRET_KEY
    bucket_name = bucket_name or BUCKET_RAW
    use_https = use_https if use_https is not None else USE_HTTPS
    manifest_path = manifest_path or os.path.join(data_dir, "file_manifest.json")

    logger.info("Connecting to MinIO at %s ...", endpoint)
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=use_https)

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info("Created bucket '%s'", bucket_name)

    manifest_file = Path(manifest_path)
    if not manifest_file.exists():
        logger.error("Manifest not found: %s", manifest_path)
        return {"success": False, "error": f"Manifest not found: {manifest_path}"}

    with open(manifest_file, "r") as f:
        manifest = json.load(f)

    bucket_objects = _get_bucket_objects(client, bucket_name)

    uploaded, skipped, failed = 0, 0, 0
    for file_info in manifest.get("files", []):
        filename = file_info["filename"]
        file_path = Path(data_dir) / filename

        if not file_path.exists():
            logger.warning("Missing file, skipping: %s", file_path)
            failed += 1
            continue

        # Skip if already uploaded with matching size
        if filename in bucket_objects:
            local_size = file_path.stat().st_size
            if local_size == bucket_objects[filename].get("size", -1):
                logger.info("Already exists, skipping: %s", filename)
                skipped += 1
                continue

        try:
            metadata = {
                "part_number": str(file_info.get("part_number", "")),
                "row_count": str(file_info.get("row_count", "")),
                "md5_hash": file_info.get("md5_hash", ""),
                "created_at": file_info.get("created_at", ""),
                "size_bytes": str(file_info.get("size_bytes", "")),
            }
            client.fput_object(bucket_name, filename, str(file_path), metadata=metadata)
            size_mb = file_path.stat().st_size / (1024 ** 2)
            logger.info("Uploaded: %s (%.2f MB)", filename, size_mb)
            uploaded += 1
        except Exception as e:
            logger.error("Failed to upload %s: %s", filename, e)
            failed += 1

    # Upload manifest
    try:
        client.fput_object(bucket_name, "file_manifest.json", str(manifest_file))
        logger.info("Uploaded manifest")
    except Exception as e:
        logger.error("Failed to upload manifest: %s", e)
        failed += 1

    logger.info("Upload done: %d uploaded, %d skipped, %d failed", uploaded, skipped, failed)
    return {"success": failed == 0, "uploaded": uploaded, "skipped": skipped, "failed": failed}


# ===========================================================================
# Scrape orchestration
# ===========================================================================

def scrape(
    zip_file,
    output_dir="data",
    progress_file="reports/progress.json",
    year_start=1982,
    year_end=2026,
    vehicle_condition="USED",
    batch_year_end=2010,
    radius=25,
    delay=1.0,
):
    """
    Scrape Carfax listings for all zip/year combos.

    Files are named like  listings_used_part_001.jsonl  or  listings_new_part_001.jsonl
    to distinguish vehicle condition. Progress is saved per zip+year key so runs
    can be resumed after interruption.
    """
    condition_tag = vehicle_condition.lower()  # "used" or "new"
    base_filename = f"listings_{condition_tag}"

    storage = FileStorageManager(output_dir, base_filename)
    zip_codes = load_zip_codes(zip_file)
    processed = get_processed_keys(progress_file)

    # Build year ranges: batch old years together, individual for recent
    year_ranges = []
    if year_start <= batch_year_end:
        year_ranges.append((year_start, min(batch_year_end, year_end)))
    for year in range(max(batch_year_end + 1, year_start), year_end + 1):
        year_ranges.append((year, year))

    total = len(zip_codes) * len(year_ranges)
    logger.info("Zips: %d | Year ranges: %d | Combos: %d | Already done: %d",
                len(zip_codes), len(year_ranges), total, len(processed))
    logger.info("Output: %s/%s_part_*.jsonl", output_dir, base_filename)

    start = datetime.now()

    try:
        for i, zip_code in enumerate(zip_codes, 1):
            zip_listings = []
            completed_keys = []

            for year_min, year_max in year_ranges:
                key = f"{zip_code}_{year_min}-{year_max}_{condition_tag}"
                if key in processed or is_processed(key, progress_file):
                    continue

                label = f"{year_min}-{year_max}" if year_min != year_max else str(year_min)
                logger.info("[%d/%d] zip=%s years=%s condition=%s",
                            i, len(zip_codes), zip_code, label, condition_tag)

                try:
                    listings = fetch_all_pages(zip_code, year_min, year_max,
                                               vehicle_condition, radius, delay=delay)
                    zip_listings.extend(listings or [])
                    completed_keys.append((key, len(listings) if listings else 0, None))
                except Exception as e:
                    logger.error("  ERROR: %s", e)
                    completed_keys.append((key, 0, str(e)))

                time.sleep(delay)

            if zip_listings:
                storage.append_batch(zip_listings)
                info = storage.get_file_info()
                logger.info("  Saved %d listings | File: %s (%.1f MB)",
                            len(zip_listings), info["filename"], info["size_mb"])

            for key, count, error in completed_keys:
                mark_completed(key, progress_file, count=count, error=error)
                processed.add(key)

    finally:
        storage.close()

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("Scrape finished in %.0fs", elapsed)


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="Carfax data pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    # -- scrape ------------------------------------------------------------
    sp = sub.add_parser("scrape", help="Scrape listings from Carfax API")
    sp.add_argument("--zip-file", default="zip_codes.txt")
    sp.add_argument("--output-dir", default="data")
    sp.add_argument("--progress-file", default="reports/progress.json")
    sp.add_argument("--year-start", type=int, default=1982)
    sp.add_argument("--year-end", type=int, default=2026)
    sp.add_argument("--condition", default="USED", choices=["USED", "NEW"])
    sp.add_argument("--batch-year-end", type=int, default=2010)
    sp.add_argument("--radius", type=int, default=25)
    sp.add_argument("--delay", type=float, default=1.0)

    # -- upload ------------------------------------------------------------
    up = sub.add_parser("upload", help="Upload JSONL files to MinIO")
    up.add_argument("--data-dir", default="data")
    up.add_argument("--manifest", default=None)
    up.add_argument("--endpoint", default=None)
    up.add_argument("--bucket", default=None)
    up.add_argument("--no-https", action="store_true")

    # -- full (scrape + upload) --------------------------------------------
    fp = sub.add_parser("full", help="Scrape then upload")
    fp.add_argument("--zip-file", default="zip_codes.txt")
    fp.add_argument("--output-dir", default="data")
    fp.add_argument("--progress-file", default="reports/progress.json")
    fp.add_argument("--year-start", type=int, default=1982)
    fp.add_argument("--year-end", type=int, default=2026)
    fp.add_argument("--condition", default="USED", choices=["USED", "NEW"])
    fp.add_argument("--batch-year-end", type=int, default=2010)
    fp.add_argument("--radius", type=int, default=25)
    fp.add_argument("--delay", type=float, default=1.0)
    fp.add_argument("--endpoint", default=None)
    fp.add_argument("--bucket", default=None)
    fp.add_argument("--no-https", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.command in ("scrape", "full"):
        scrape(
            zip_file=args.zip_file,
            output_dir=args.output_dir,
            progress_file=args.progress_file,
            year_start=args.year_start,
            year_end=args.year_end,
            vehicle_condition=args.condition,
            batch_year_end=args.batch_year_end,
            radius=args.radius,
            delay=args.delay,
        )

    if args.command in ("upload", "full"):
        data_dir = args.data_dir if hasattr(args, "data_dir") else args.output_dir
        upload_to_minio(
            data_dir=data_dir,
            manifest_path=getattr(args, "manifest", None),
            endpoint=getattr(args, "endpoint", None),
            bucket_name=getattr(args, "bucket", None),
            use_https=not getattr(args, "no_https", False),
        )


if __name__ == "__main__":
    main()
