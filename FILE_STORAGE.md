# File Storage System

The DataLake-SP4850 project now includes an automatic file storage management system designed to handle large datasets efficiently and prepare metadata for object storage (Minio, S3, etc.).

## Features

### 1. Automatic File Rotation

Files are automatically rotated when they reach 512 MB:
- Output files are created as `listings_part_001.jsonl`, `listings_part_002.jsonl`, etc.
- Each file contains one JSON listing per line for streamlined processing
- Rotation is transparent to the user—just append listings and files are managed automatically

### 2. Metadata Tracking

All file metadata is tracked in `file_manifest.json`:

```json
{
  "files": [
    {
      "part_number": 1,
      "filename": "listings_part_001.jsonl",
      "filepath": "/absolute/path/to/listings_part_001.jsonl",
      "size_bytes": 536870912,
      "row_count": 125000,
      "md5_hash": "abc123def456...",
      "created_at": "2026-03-19T10:30:45.123456",
      "is_complete": true
    },
    {
      "part_number": 2,
      "filename": "listings_part_002.jsonl",
      "filepath": "/absolute/path/to/listings_part_002.jsonl",
      "size_bytes": 268435456,
      "row_count": 65000,
      "md5_hash": "xyz789abc123...",
      "created_at": "2026-03-19T12:15:30.654321",
      "is_complete": false
    }
  ],
  "created_at": "2026-03-19T10:00:00.000000",
  "last_updated": "2026-03-19T15:45:30.000000"
}
```

### 3. Resumable Storage

The storage manager can recover from interruptions:
- If a scraping run is interrupted, restarting will:
  1. Find the highest part number from the manifest
  2. Check if it's under the size limit
  3. Resume appending to the current file (or start a new one if full)

### 4. File Integrity

Each file includes:
- **MD5 Hash**: For integrity verification and Minio upload
- **Row Count**: For data validation
- **Byte Size**: For storage planning
- **Timestamps**: For audit trails

## Usage

### Basic Usage

The file storage is transparent in the main scraper:

```python
from file_ops import get_storage_manager

# Initialize (auto-creates 512 MB rotation)
storage = get_storage_manager(output_dir=".", base_filename="listings")

# Write listings (handles rotation automatically)
listings = [...]
count = append_listings_to_file(listings, storage_manager=storage)

# Close when done
from file_ops import close_storage_manager
close_storage_manager()
```

### Direct Usage

```python
from file_storage import FileStorageManager

# Create manager
manager = FileStorageManager(
    output_dir=".",
    base_filename="listings",
    manifest_file="file_manifest.json"
)

# Single listing
manager.append_listing({"id": 1, "title": "Car", ...})

# Batch (preferred)
manager.append_listings_batch([
    {"id": 1, "title": "Car 1", ...},
    {"id": 2, "title": "Car 2", ...},
])

# Get current file info
info = manager.get_file_info()
print(f"Current file: {info['filename']} ({info['size_mb']:.1f} MB)")

# Get summary
summary = manager.get_manifest_summary()
print(f"Total: {summary['total_files']} files, {summary['total_rows']} rows")

# Close and finalize
manager.close()
```

### Context Manager

For automatic cleanup:

```python
from file_storage import FileStorageManager

with FileStorageManager(output_dir=".") as manager:
    manager.append_listings_batch(listings)
    # Automatically closes on exit
```

## Configuration

Adjust the file size limit by modifying `file_storage.py`:

```python
class FileStorageManager:
    FILE_SIZE_LIMIT = 512 * 1024 * 1024  # Change this (in bytes)
```

Or set it per instance:

```python
manager = FileStorageManager(...)
manager.FILE_SIZE_LIMIT = 256 * 1024 * 1024  # 256 MB
```

## Minio Integration (Future)

The manifest format is designed for easy porting to Minio. Each file entry includes all metadata needed for:

- **Uploading**: `filepath`, `size_bytes`, `md5_hash`
- **Verification**: Check local MD5 against remote
- **Tracking**: `created_at`, `row_count` for accounting
- **Organization**: `part_number`, `filename` for naming scheme

Example future usage:
```python
# High-level (pseudocode)
manifest = manager.get_manifest()
for file_info in manifest['files']:
    minio_client.put_object(
        bucket="datalake",
        object_name=file_info['filename'],
        file_path=file_info['filepath'],
        metadata={
            "part": str(file_info['part_number']),
            "rows": str(file_info['row_count']),
            "md5": file_info['md5_hash']
        }
    )
```

## Output Structure

```
.
├── listings_part_001.jsonl      # 512 MB
├── listings_part_002.jsonl      # 512 MB
├── listings_part_003.jsonl      # 256 MB (partial)
└── file_manifest.json           # Metadata for all files
```

## Testing

Run the test suite:

```bash
python test_file_storage.py
```

This validates:
- File rotation at size limits
- MD5 hash generation
- Manifest creation
- Metadata accuracy
- File existence verification

## Performance Notes

- **Batch writes preferred**: `append_listings_batch()` is more efficient than multiple `append_listing()` calls
- **MD5 hashing**: Computed incrementally as data is written (minimal overhead)
- **Memory**: Streaming writes keep memory usage constant regardless of file size
