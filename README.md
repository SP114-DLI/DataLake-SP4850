# DataLake-SP4850

Vehicle data lake implementation built around Carfax listing data, organized into standard data lake layers.

## Project Structure

```
├── scrape/                  # Data collection (Carfax API scraping)
│   ├── main.py              # Orchestration loop (entry point)
│   ├── api.py               # Carfax API client with retry logic
│   ├── config.py            # API constants and credential loading
│   └── progress.py          # Resumable progress tracking
│
├── raw/                     # Bronze layer — raw data ingestion & storage
│   ├── config.py            # S3/MinIO connection settings
│   ├── storage.py           # JSONL file rotation manager (512 MB)
│   ├── upload.py            # MinIO upload with deduplication
│   ├── s3.py                # Boto3 S3 operations (buckets, objects)
│   ├── duplicate_counter.py # Cross-file duplicate detection
│   └── split_file.py        # JSONL file splitting utility
│
├── silver/                  # Silver layer — cleaning & transformation (planned)
├── gold/                    # Gold layer — aggregations & analytics (planned)
│
├── tests/                   # Test suite
│   └── test_storage.py      # File rotation and manifest tests
│
├── zip_codes.txt            # Georgia ZIP codes (scraping input)
├── user_config.py           # Carfax API credentials (not in repo)
├── requirements.txt         # Python dependencies
├── FILE_STORAGE.md          # File storage system documentation
└── README.md
```

## Setup

1. Create `user_config.py` with your Carfax API credentials:
```python
COOKIES = { ... }  # Your session cookies
HEADERS = { ... }  # Your request headers
```

2. Create `zip_codes.txt` with one ZIP code per line.

3. Install dependencies:
```bash
pip install requests minio boto3
```

## Usage

### Run the scraper
```bash
python -m scrape.main
```

Parameters are configured in `scrape/main.py`:
- `year_start` / `year_end`: Model year range (default 1982-2026)
- `vehicle_condition`: "USED" or "NEW"
- `batch_year_end`: Years <= this are fetched as one batch (default 2010)
- `radius`: Search radius in miles (default 25)
- `delay`: Seconds between requests (default 0)

### Upload data to MinIO
```bash
python -m raw.upload
python -m raw.upload --endpoint my-server.com --bucket my-bucket
```

### S3 bucket management
```python
from raw.s3 import list_buckets, create_bucket, setup_lake_buckets
setup_lake_buckets()  # Creates lakebronze, lakesilver, lakegold
```

### Count duplicates
```bash
python -m raw.duplicate_counter --data-dir data
```

### Run tests
```bash
python -m tests.test_storage
```

## Data Lake Layers

| Layer | Bucket | Description |
|-------|--------|-------------|
| **Raw** | `lakeraw` | Unprocessed scraped JSONL files, uploaded as-is |
| **Bronze** | `lakebronze` | Validated raw data with schema enforcement |
| **Silver** | `lakesilver` | Deduplicated, cleaned, normalized listings |
| **Gold** | `lakegold` | Aggregated analytics (pricing trends, market stats) |

## File Storage

Output files are automatically rotated at 512 MB:
- `data/listings_part_001.jsonl` (up to 512 MB)
- `data/listings_part_002.jsonl` (up to 512 MB)
- `data/file_manifest.json` (metadata for all files)

Progress is tracked in `progress.json` per ZIP/year combination, allowing interrupted runs to resume.


## Object Store Setup
1. Download minio.exe server binary from www.min.io

2. Obtain a license from minio to run the server and create a local file directory to host the server

3. Configure config file or environmental variables such as MINIO_LICENSE or MINIO_ADDRESS to customize server features and information

4. Start the server from the command line with the appropriate flags and path to the server directory
