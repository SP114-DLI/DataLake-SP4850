"""Main module for orchestrating data upload to remote server."""

import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

from minio import Minio


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Configure logging for the send_data module.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    handler = logging.StreamHandler()
    handler.setLevel(getattr(logging, log_level.upper()))
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger


logger = setup_logging()


def send_all_data(
    endpoint: str = "sp114api.loclx.io",
    access_key: str = "SP114",
    secret_key: str = "DataLakeImplementation",
    bucket_name: str = "lakeraw",
    manifest_path: str = "data/file_manifest.json",
    data_dir: str = "data",
    use_https: bool = True
) -> Dict:
    """
    Send all data files and metadata to the MinIO server.
    
    Args:
        endpoint: MinIO server endpoint (without https://)
        access_key: Access key for authentication
        secret_key: Secret key for authentication
        bucket_name: Name of the bucket to store files in
        manifest_path: Path to the file_manifest.json
        data_dir: Directory containing data files
        use_https: Whether to use HTTPS
    
    Returns:
        Dictionary with upload results
    """
    from minio import Minio
    from minio.error import S3Error
    
    logger.info(f"Initializing MinIO connection to {endpoint}...")
    
    try:
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=use_https
        )
        logger.info("MinIO client connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        return {"success": False, "error": f"Connection failed: {e}"}
    
    # Ensure bucket exists
    try:
        if not client.bucket_exists(bucket_name):
            logger.info(f"Creating bucket '{bucket_name}'...")
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists")
    except S3Error as e:
        logger.error(f"Bucket operation failed: {e}")
        return {"success": False, "error": f"Bucket operation failed: {e}"}
    
    # Load manifest
    manifest_file = Path(manifest_path)
    if not manifest_file.exists():
        logger.error(f"Manifest file not found: {manifest_path}")
        return {"success": False, "error": f"Manifest not found: {manifest_path}"}
    
    try:
        with open(manifest_file, 'r') as f:
            manifest = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load manifest: {e}")
        return {"success": False, "error": f"Failed to load manifest: {e}"}
    
    # Upload data files
    stats = {
        "timestamp": datetime.now().isoformat(),
        "endpoint": endpoint,
        "bucket": bucket_name,
        "total_files": 0,
        "successful_uploads": 0,
        "failed_uploads": 0,
        "files": [],
        "manifest_uploaded": False
    }
    
    files_to_upload = manifest.get("files", [])
    stats["total_files"] = len(files_to_upload)
    
    logger.info(f"Preparing to upload {stats['total_files']} files...")
    
    for file_info in files_to_upload:
        filename = file_info.get("filename")
        file_path = Path(data_dir) / filename
        
        if not file_path.exists():
            logger.warning(f"File not found, skipping: {file_path}")
            stats["failed_uploads"] += 1
            stats["files"].append({
                "filename": filename,
                "success": False,
                "error": "File not found"
            })
            continue
        
        try:
            file_size_mb = file_path.stat().st_size / (1024 ** 2)
            logger.info(f"Uploading: {filename} ({file_size_mb:.2f} MB)...")
            
            # Prepare metadata tags
            metadata = {
                "part_number": str(file_info.get("part_number", "")),
                "row_count": str(file_info.get("row_count", "")),
                "md5_hash": file_info.get("md5_hash", ""),
                "created_at": file_info.get("created_at", ""),
                "size_bytes": str(file_info.get("size_bytes", ""))
            }
            
            # Upload file
            client.fput_object(
                bucket_name,
                filename,
                str(file_path),
                metadata=metadata
            )
            
            logger.info(f"✓ Successfully uploaded: {filename}")
            stats["successful_uploads"] += 1
            stats["files"].append({
                "filename": filename,
                "success": True,
                "size_mb": file_size_mb,
                "metadata": metadata
            })
            
        except S3Error as e:
            logger.error(f"✗ Failed to upload {filename}: {e}")
            stats["failed_uploads"] += 1
            stats["files"].append({
                "filename": filename,
                "success": False,
                "error": str(e)
            })
        except Exception as e:
            logger.error(f"✗ Unexpected error uploading {filename}: {e}")
            stats["failed_uploads"] += 1
            stats["files"].append({
                "filename": filename,
                "success": False,
                "error": str(e)
            })
    
    # Upload manifest file
    try:
        logger.info("Uploading manifest file...")
        client.fput_object(
            bucket_name,
            "file_manifest.json",
            str(manifest_file)
        )
        logger.info("✓ Successfully uploaded manifest: file_manifest.json")
        stats["manifest_uploaded"] = True
    except Exception as e:
        logger.error(f"✗ Failed to upload manifest: {e}")
        stats["manifest_uploaded"] = False
    
    # Summary
    stats["success"] = stats["failed_uploads"] == 0 and stats["manifest_uploaded"]
    
    logger.info("=" * 60)
    logger.info(f"Upload Summary:")
    logger.info(f"  Total files: {stats['total_files']}")
    logger.info(f"  Successful: {stats['successful_uploads']}")
    logger.info(f"  Failed: {stats['failed_uploads']}")
    logger.info(f"  Manifest uploaded: {stats['manifest_uploaded']}")
    logger.info(f"  Overall status: {'SUCCESS' if stats['success'] else 'FAILED'}")
    logger.info("=" * 60)
    
    return stats


def main():
    """Command-line entry point for uploading data."""
    parser = argparse.ArgumentParser(
        description="Send all data files to MinIO server"
    )
    parser.add_argument(
        "--endpoint",
        default="sp114api.loclx.io",
        help="MinIO server endpoint (default: sp114api.loclx.io)"
    )
    parser.add_argument(
        "--access-key",
        default="SP114",
        help="MinIO access key (default: SP114)"
    )
    parser.add_argument(
        "--secret-key",
        default="DataLakeImplementation",
        help="MinIO secret key (default: DataLakeImplementation)"
    )
    parser.add_argument(
        "--bucket",
        default="datalake",
        help="Bucket name (default: datalake)"
    )
    parser.add_argument(
        "--manifest",
        default="data/file_manifest.json",
        help="Path to manifest file (default: data/file_manifest.json)"
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Path to data directory (default: data)"
    )
    parser.add_argument(
        "--no-https",
        action="store_true",
        help="Disable HTTPS (use HTTP instead)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Reconfigure logging if needed
    global logger
    logger = setup_logging(args.log_level)
    
    result = send_all_data(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        bucket_name=args.bucket,
        manifest_path=args.manifest,
        data_dir=args.data_dir,
        use_https=not args.no_https
    )
    
    # Save results to file for reference
    results_file = Path("send_data_results.json")
    try:
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2)
        logger.info(f"Results saved to {results_file}")
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
    
    return 0 if result["success"] else 1


if __name__ == "__main__":
    exit(main())
