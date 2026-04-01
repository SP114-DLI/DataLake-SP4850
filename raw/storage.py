"""File storage management with automatic rotation and metadata tracking."""

import json
import hashlib
from pathlib import Path
from datetime import datetime

from raw.config import FILE_SIZE_LIMIT, MANIFEST_SAVE_INTERVAL


class FileStorageManager:
    """
    Manages JSONL file storage with automatic rotation at a size limit.

    Files are rotated at FILE_SIZE_LIMIT (default 512 MB). Metadata is
    maintained in a manifest file for object storage integration.
    """

    def __init__(self, output_dir=".", base_filename="listings",
                 manifest_file="file_manifest.json", file_size_limit=None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.base_filename = base_filename
        self.manifest_file = self.output_dir / manifest_file
        self.file_size_limit = file_size_limit or FILE_SIZE_LIMIT

        self.current_file_handle = None
        self.current_file_path = None
        self.current_file_size = 0
        self.current_file_row_count = 0
        self.current_part_num = 1
        self.current_file_hash = hashlib.md5()

        self._manifest_save_interval = MANIFEST_SAVE_INTERVAL
        self._rows_since_save = 0

        self.manifest = self._load_manifest()
        self._recover_state()

    def _load_manifest(self):
        """Load existing manifest or create an empty one."""
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {"files": [], "created_at": datetime.now().isoformat(), "last_updated": None}

    def _save_manifest(self):
        """Save manifest to disk."""
        self.manifest["last_updated"] = datetime.now().isoformat()
        with open(self.manifest_file, "w") as f:
            json.dump(self.manifest, f, indent=2)

    def _recover_state(self):
        """Recover state from manifest to resume after interruption."""
        if not self.manifest["files"]:
            return

        max_part = max(f["part_number"] for f in self.manifest["files"])
        last_file = next(f for f in self.manifest["files"] if f["part_number"] == max_part)

        self.current_part_num = max_part
        self.current_file_path = self.output_dir / last_file["filename"]

        if last_file["size_bytes"] < self.file_size_limit and self.current_file_path.exists():
            self.current_file_size = last_file["size_bytes"]
            self.current_file_row_count = last_file["row_count"]
            self.current_file_handle = open(self.current_file_path, "ab")
        else:
            self.current_file_handle = None

    def _get_filename(self):
        """Generate filename for current part number."""
        return f"{self.base_filename}_part_{self.current_part_num:03d}.jsonl"

    def _open_new_file(self):
        """Finalize current file and open a new one."""
        if self.current_file_handle and not self.current_file_handle.closed:
            self._finalize_current_file()

        self.current_part_num += 1
        self.current_file_path = self.output_dir / self._get_filename()
        self.current_file_handle = open(self.current_file_path, "wb")
        self.current_file_size = 0
        self.current_file_row_count = 0
        self.current_file_hash = hashlib.md5()

    def _finalize_current_file(self):
        """Save metadata for current file and close it."""
        if not self.current_file_handle or self.current_file_handle.closed:
            return

        self.current_file_handle.close()
        self.manifest["files"].append({
            "part_number": self.current_part_num,
            "filename": self.current_file_path.name,
            "filepath": str(self.current_file_path),
            "size_bytes": self.current_file_size,
            "row_count": self.current_file_row_count,
            "md5_hash": self.current_file_hash.hexdigest(),
            "created_at": datetime.now().isoformat(),
            "is_complete": self.current_file_size >= self.file_size_limit,
        })
        self._save_manifest()

    def append_listing(self, listing):
        """Append a single listing as a JSON line, rotating files as needed."""
        if self.current_file_handle is None:
            self.current_file_path = self.output_dir / self._get_filename()
            self.current_file_handle = open(self.current_file_path, "wb")
            self.current_file_size = 0
            self.current_file_row_count = 0
            self.current_file_hash = hashlib.md5()

        line = json.dumps(listing, separators=(",", ":")).encode("utf-8") + b"\n"

        if self.current_file_size + len(line) > self.file_size_limit and self.current_file_size > 0:
            self._open_new_file()

        self.current_file_handle.write(line)
        self.current_file_size += len(line)
        self.current_file_row_count += 1
        self.current_file_hash.update(line)
        self._rows_since_save += 1

        if self._rows_since_save >= self._manifest_save_interval:
            self.flush()
            self._save_manifest()
            self._rows_since_save = 0

        return True

    def append_listings_batch(self, listings):
        """Append multiple listings. Returns count written."""
        return sum(1 for listing in listings if self.append_listing(listing))

    def flush(self):
        """Flush current file handle to disk."""
        if self.current_file_handle and not self.current_file_handle.closed:
            self.current_file_handle.flush()

    def close(self):
        """Finalize and close the current file."""
        self._finalize_current_file()
        self.current_file_handle = None

    def get_file_info(self):
        """Get info about the file currently being written."""
        if self.current_file_path is None:
            return None
        return {
            "part_number": self.current_part_num,
            "filename": self.current_file_path.name,
            "size_bytes": self.current_file_size,
            "row_count": self.current_file_row_count,
            "size_mb": self.current_file_size / (1024 * 1024),
        }

    def get_manifest_summary(self):
        """Get aggregate stats across all files in the manifest."""
        files = self.manifest["files"]
        if not files:
            return {"total_files": 0, "total_rows": 0, "total_size_bytes": 0,
                    "total_size_mb": 0, "average_file_size_mb": 0}

        total_rows = sum(f["row_count"] for f in files)
        total_size = sum(f["size_bytes"] for f in files)
        return {
            "total_files": len(files),
            "total_rows": total_rows,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "average_file_size_mb": (total_size / len(files)) / (1024 * 1024),
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
