"""Configuration for raw data storage and object storage connections."""

# MinIO / S3 connection defaults
ENDPOINT = "sp114api.loclx.io"
ACCESS_KEY = "SP114"
SECRET_KEY = "DataLakeImplementation"
REGION = "us-east-1"
USE_HTTPS = False

# Data lake bucket names (lakeraw serves as the bronze layer)
BUCKET_RAW = "lakeraw"
BUCKET_SILVER = "lakesilver"
BUCKET_GOLD = "lakegold"

# File storage defaults
FILE_SIZE_LIMIT = 512 * 1024 * 1024  # 512 MB in bytes
MANIFEST_SAVE_INTERVAL = 10  # rows between periodic manifest saves
