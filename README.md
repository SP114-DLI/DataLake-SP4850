# DataLake-SP4850

Vehicle data scraper for Carfax listings by ZIP code and model year.

## Project Structure

```
├── scrapev2.py          # Main entry point
├── scraper.py           # Core scraping logic (API requests, pagination)
├── file_ops.py          # File I/O and progress tracking
├── file_storage.py      # Automatic file rotation and metadata (for Minio)
├── constants.py         # Configuration and API constants
├── user_config.py       # User-provided API credentials (not in repo)
├── zip_codes.txt        # List of ZIP codes to scrape
├── progress.json        # Progress tracking with timestamps
├── data/                # Output directory
│   ├── listings_part_001.jsonl      # Output files (auto-rotated at 512 MB)
│   ├── listings_part_002.jsonl
│   └── file_manifest.json           # File metadata for object storage
├── utils/               # Utility scripts
│   └── split_file.py    # Split JSONL files into chunks
├── FILE_STORAGE.md      # File storage system documentation
└── README.md            # This file
```

## Setup

1. Create `user_config.py` with your Carfax API credentials:
```python
COOKIES = { ... }  # Your session cookies
HEADERS = { ... }  # Your request headers
```

2. Create `zip_codes.txt` with one ZIP code per line:
```
30002
30003
30004
```

3. Install dependencies:
```bash
pip install requests
```

## Usage

Run the main scraper:
```bash
python scrapev2.py
```

All output files will be created in the `data/` directory.

Customize parameters in `scrapev2.py` or pass them directly:
- `year_start`: First model year (default 1982)
- `year_end`: Last model year (default 2026)
- `vehicle_condition`: "USED" or "NEW" (default "USED")
- `batch_year_end`: Years ≤ this are fetched as one batch; after are individual (default 2005)
- `radius`: Search radius in miles (default 25)
- `delay`: Seconds between page requests (default 1.0)
- `output_dir`: Output directory (default "data")

## File Storage

**Automatic File Rotation**: Files are automatically rotated at 512 MB to manage large datasets.

Output files are created in `data/` as:
- `listings_part_001.jsonl` (0-512 MB)
- `listings_part_002.jsonl` (0-512 MB)
- `listings_part_003.jsonl` (etc...)

All file metadata is tracked in `data/file_manifest.json`:
```json
{
  "files": [
    {
      "part_number": 1,
      "filename": "listings_part_001.jsonl",
      "filepath": "/absolute/path/to/data/listings_part_001.jsonl",
      "size_bytes": 536870912,
      "row_count": 125000,
      "md5_hash": "abc123def456...",
      "created_at": "2026-03-19T10:30:45.123456",
      "is_complete": true
    }
  ],
  "created_at": "2026-03-19T10:00:00.000000",
  "last_updated": "2026-03-19T14:45:30.000000"
}
```

This metadata is prepared for porting to Minio or other object storage systems.

## Progress Tracking

Progress is saved in `progress.json` with timestamps and counts:
```json
{
  "30002_1982-2005": {
    "completed_at": "2026-03-19T10:30:45.123456",
    "count": 1250,
    "error": null
  }
}
```

This allows resuming interrupted runs—just re-run the script.

## Architecture

- **Modular design**: Each module has a single responsibility
  - `scraper.py` — API interactions & pagination
  - `file_ops.py` — Data persistence & progress tracking
  - `file_storage.py` — Automatic file rotation & Minio-ready metadata
  - `constants.py` — Configuration
  - `scrapev2.py` — Orchestration

- **Resumable**: Progress tracking survives interruptions
- **Batched writes**: Listings for each ZIP code are batched before writing
- **Storage-ready**: File manifest prepared for object storage (Minio, S3, etc.)
- **Organized output**: All generated files stored in `data/` directory
