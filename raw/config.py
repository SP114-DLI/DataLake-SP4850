"""Configuration for raw data storage and object storage connections."""

import os

# MinIO / S3 connection. Set via environment variables; no defaults for secrets.
ENDPOINT = os.getenv("S3_ENDPOINT", "")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
REGION = os.getenv("S3_REGION", "us-east-1")
USE_HTTPS = os.getenv("S3_USE_HTTPS", "false").lower() == "true"

# Data lake bucket names (lakeraw serves as the bronze layer)
BUCKET_RAW = "lakeraw"
BUCKET_SILVER = "lakesilver"
BUCKET_GOLD = "lakegold"

# File storage defaults
FILE_SIZE_LIMIT = 512 * 1024 * 1024  # 512 MB in bytes
MANIFEST_SAVE_INTERVAL = 10  # rows between periodic manifest saves
