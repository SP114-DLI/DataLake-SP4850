"""Upload data files to MinIO/S3 object storage."""

import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

from minio import Minio
from minio.error import S3Error

from raw.config import ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET_RAW, USE_HTTPS

logger = logging.getLogger(__name__)


def _setup_logging(level="INFO"):
    """Configure module logger with console output."""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper()))


def get_bucket_objects(client, bucket_name):
    """Get dict of all objects in a bucket: {filename: {size, etag}}."""
    objects = {}
    try:
        for obj in client.list_objects(bucket_name):
            objects[obj.object_name] = {"size": obj.size, "etag": obj.etag}
    except Exception as e:
        logger.warning(f"Failed to list bucket objects: {e}")
    return objects


def send_all_data(
    endpoint=None,
    access_key=None,
    secret_key=None,
    bucket_name=None,
    manifest_path="data/file_manifest.json",
    data_dir="data",
    use_https=None,
):
    """
    Send all data files and manifest to MinIO.

    Returns:
        dict with upload stats and success status
    """
    endpoint = endpoint or ENDPOINT
    access_key = access_key or ACCESS_KEY
    secret_key = secret_key or SECRET_KEY
    bucket_name = bucket_name or BUCKET_RAW
    use_https = use_https if use_https is not None else USE_HTTPS

    logger.info(f"Connecting to MinIO at {endpoint}...")
    try:
        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=use_https)
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return {"success": False, "error": str(e)}

    # Ensure bucket exists
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket '{bucket_name}'")
    except S3Error as e:
        logger.error(f"Bucket operation failed: {e}")
        return {"success": False, "error": str(e)}

    # Load manifest
    manifest_file = Path(manifest_path)
    if not manifest_file.exists():
        return {"success": False, "error": f"Manifest not found: {manifest_path}"}

    with open(manifest_file, "r") as f:
        manifest = json.load(f)

    bucket_objects = get_bucket_objects(client, bucket_name)

    stats = {
        "timestamp": datetime.now().isoformat(),
        "endpoint": endpoint,
        "bucket": bucket_name,
        "total_files": 0,
        "successful_uploads": 0,
        "skipped_uploads": 0,
        "failed_uploads": 0,
        "files": [],
        "manifest_uploaded": False,
    }

    files_to_upload = manifest.get("files", [])
    stats["total_files"] = len(files_to_upload)
    logger.info(f"Preparing to upload {stats['total_files']} files...")

    for file_info in files_to_upload:
        filename = file_info["filename"]
        file_path = Path(data_dir) / filename

        if not file_path.exists():
            logger.warning(f"File not found, skipping: {file_path}")
            stats["failed_uploads"] += 1
            stats["files"].append({"filename": filename, "success": False, "error": "File not found"})
            continue

        # Skip if already uploaded with matching size
        if filename in bucket_objects:
            local_size = file_path.stat().st_size
            remote_size = bucket_objects[filename].get("size", -1)
            if local_size == remote_size:
                logger.info(f"Skipping (already exists): {filename}")
                stats["skipped_uploads"] += 1
                stats["files"].append({"filename": filename, "success": True, "skipped": True})
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
            logger.info(f"Uploaded: {filename} ({size_mb:.2f} MB)")
            stats["successful_uploads"] += 1
            stats["files"].append({"filename": filename, "success": True, "size_mb": size_mb})
        except Exception as e:
            logger.error(f"Failed to upload {filename}: {e}")
            stats["failed_uploads"] += 1
            stats["files"].append({"filename": filename, "success": False, "error": str(e)})

    # Upload manifest file
    try:
        client.fput_object(bucket_name, "file_manifest.json", str(manifest_file))
        logger.info("Uploaded manifest: file_manifest.json")
        stats["manifest_uploaded"] = True
    except Exception as e:
        logger.error(f"Failed to upload manifest: {e}")

    stats["success"] = stats["failed_uploads"] == 0 and stats["manifest_uploaded"]
    logger.info(f"Upload complete: {stats['successful_uploads']} uploaded, "
                f"{stats['skipped_uploads']} skipped, {stats['failed_uploads']} failed")
    return stats


def main():
    """Command-line entry point for uploading data."""
    parser = argparse.ArgumentParser(description="Upload data files to MinIO")
    parser.add_argument("--endpoint", default=None, help="MinIO endpoint")
    parser.add_argument("--access-key", default=None, help="MinIO access key")
    parser.add_argument("--secret-key", default=None, help="MinIO secret key")
    parser.add_argument("--bucket", default=None, help="Bucket name")
    parser.add_argument("--manifest", default="data/file_manifest.json", help="Manifest path")
    parser.add_argument("--data-dir", default="data", help="Data directory")
    parser.add_argument("--no-https", action="store_true", help="Use HTTP instead of HTTPS")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

    args = parser.parse_args()
    _setup_logging(args.log_level)

    result = send_all_data(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        bucket_name=args.bucket,
        manifest_path=args.manifest,
        data_dir=args.data_dir,
        use_https=not args.no_https,
    )

    results_file = Path("send_data_results.json")
    with open(results_file, "w") as f:
        json.dump(result, f, indent=2)
    logger.info(f"Results saved to {results_file}")

    return 0 if result["success"] else 1


if __name__ == "__main__":
    exit(main())
