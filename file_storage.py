"""File storage management with automatic rotation and metadata tracking."""

import json
import os
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List


class FileStorageManager:
    """
    Manages JSONL file storage with automatic rotation at size limit.
    
    When a file reaches the size limit (default 512 MB), a new file is created
    with an incremented part number. Metadata is maintained in a manifest file
    for eventual porting to object storage (e.g., Minio).
    """
    
    FILE_SIZE_LIMIT = 512 * 1024 * 1024  # 512 MB in bytes
    
    def __init__(self, output_dir: str = ".", base_filename: str = "listings", manifest_file: str = "file_manifest.json"):
        """
        Initialize the file storage manager.
        
        Args:
            output_dir: Directory to store output files
            base_filename: Base name for output files (e.g., "listings" → listings_part_001.jsonl)
            manifest_file: Name of the metadata manifest file
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_filename = base_filename
        self.manifest_file = self.output_dir / manifest_file
        
        self.current_file_handle = None
        self.current_file_path = None
        self.current_file_size = 0
        self.current_file_row_count = 0
        self.current_part_num = 1
        
        self.manifest = self._load_manifest()
        self.current_file_hash = hashlib.md5()
        
        # Recover state from manifest if files already exist
        self._recover_state()
    
    def _load_manifest(self) -> Dict:
        """Load existing manifest or create empty one."""
        if self.manifest_file.exists():
            try:
                with open(self.manifest_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {"files": [], "created_at": datetime.now().isoformat(), "last_updated": None}
        return {"files": [], "created_at": datetime.now().isoformat(), "last_updated": None}
    
    def _save_manifest(self):
        """Save manifest to file."""
        self.manifest["last_updated"] = datetime.now().isoformat()
        with open(self.manifest_file, 'w') as f:
            json.dump(self.manifest, f, indent=2)
    
    def _recover_state(self):
        """Recover state from manifest (find highest part number and current file)."""
        if not self.manifest["files"]:
            return
        
        # Find highest part number
        max_part = max(f["part_number"] for f in self.manifest["files"])
        last_file = next(f for f in self.manifest["files"] if f["part_number"] == max_part)
        
        self.current_part_num = max_part
        self.current_file_path = self.output_dir / last_file["filename"]
        
        # If the last file hasn't reached size limit, reopen it for appending
        if last_file["size_bytes"] < self.FILE_SIZE_LIMIT and self.current_file_path.exists():
            self.current_file_size = last_file["size_bytes"]
            self.current_file_row_count = last_file["row_count"]
            self.current_file_handle = open(self.current_file_path, 'ab')
        else:
            # Last file is full, start a new one
            self.current_file_handle = None
    
    def _get_next_filename(self) -> str:
        """Generate next filename with part number."""
        return f"{self.base_filename}_part_{self.current_part_num:03d}.jsonl"
    
    def _open_new_file(self):
        """Close current file (if open) and open a new one."""
        if self.current_file_handle and not self.current_file_handle.closed:
            self._finalize_current_file()
        
        self.current_part_num += 1
        self.current_file_path = self.output_dir / self._get_next_filename()
        self.current_file_handle = open(self.current_file_path, 'wb')
        self.current_file_size = 0
        self.current_file_row_count = 0
        self.current_file_hash = hashlib.md5()
    
    def _finalize_current_file(self):
        """Save metadata and close current file."""
        if not self.current_file_handle or self.current_file_handle.closed:
            return
        
        self.current_file_handle.close()
        
        # Add to manifest
        file_entry = {
            "part_number": self.current_part_num,
            "filename": self.current_file_path.name,
            "filepath": str(self.current_file_path),
            "size_bytes": self.current_file_size,
            "row_count": self.current_file_row_count,
            "md5_hash": self.current_file_hash.hexdigest(),
            "created_at": datetime.now().isoformat(),
            "is_complete": self.current_file_size >= self.FILE_SIZE_LIMIT
        }
        
        self.manifest["files"].append(file_entry)
        self._save_manifest()
    
    def append_listing(self, listing: Dict) -> bool:
        """
        Append a single listing as JSON line.
        
        Automatically rotates file when size limit is reached.
        
        Args:
            listing: Dictionary representation of a listing
            
        Returns:
            True if written successfully, False if rotation occurred
        """
        import json as json_module
        
        # Open first file if not already open
        if self.current_file_handle is None:
            self.current_file_path = self.output_dir / self._get_next_filename()
            self.current_file_handle = open(self.current_file_path, 'wb')
            self.current_file_size = 0
            self.current_file_row_count = 0
            self.current_file_hash = hashlib.md5()
        
        # Serialize listing
        line = json_module.dumps(listing, separators=(",", ":")).encode('utf-8') + b'\n'
        line_size = len(line)
        
        # Check if adding this line would exceed limit
        if self.current_file_size + line_size > self.FILE_SIZE_LIMIT and self.current_file_size > 0:
            self._open_new_file()
            line = json_module.dumps(listing, separators=(",", ":")).encode('utf-8') + b'\n'
            line_size = len(line)
        
        # Write line
        self.current_file_handle.write(line)
        self.current_file_size += line_size
        self.current_file_row_count += 1
        self.current_file_hash.update(line)
        
        return True
    
    def append_listings_batch(self, listings: List[Dict]) -> int:
        """
        Append multiple listings at once.
        
        Args:
            listings: List of listing dictionaries
            
        Returns:
            Number of listings written
        """
        count = 0
        for listing in listings:
            if self.append_listing(listing):
                count += 1
        return count
    
    def flush(self):
        """Flush current file handle."""
        if self.current_file_handle and not self.current_file_handle.closed:
            self.current_file_handle.flush()
    
    def close(self):
        """Finalize and close current file."""
        self._finalize_current_file()
        self.current_file_handle = None
    
    def get_manifest(self) -> Dict:
        """Get current manifest."""
        return self.manifest
    
    def get_file_info(self) -> Optional[Dict]:
        """Get info about current file being written."""
        if self.current_file_path is None:
            return None
        
        return {
            "part_number": self.current_part_num,
            "filename": self.current_file_path.name,
            "size_bytes": self.current_file_size,
            "row_count": self.current_file_row_count,
            "size_mb": self.current_file_size / (1024 * 1024)
        }
    
    def get_manifest_summary(self) -> Dict:
        """Get summary of all files in manifest."""
        if not self.manifest["files"]:
            return {
                "total_files": 0,
                "total_rows": 0,
                "total_size_bytes": 0,
                "total_size_mb": 0
            }
        
        total_rows = sum(f["row_count"] for f in self.manifest["files"])
        total_size = sum(f["size_bytes"] for f in self.manifest["files"])
        
        return {
            "total_files": len(self.manifest["files"]),
            "total_rows": total_rows,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "average_file_size_mb": (total_size / len(self.manifest["files"])) / (1024 * 1024) if self.manifest["files"] else 0
        }
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
