"""File operations for progress tracking and data persistence."""

import json
import os
from datetime import datetime
from file_storage import FileStorageManager


# Global file storage manager instance
_storage_manager = None


def get_storage_manager(output_dir=".", base_filename="listings", manifest_file="file_manifest.json"):
    """Get or create the global file storage manager."""
    global _storage_manager
    if _storage_manager is None:
        _storage_manager = FileStorageManager(output_dir, base_filename, manifest_file)
    return _storage_manager


def append_listings_to_file(listings, storage_manager=None):
    """
    Append listings using FileStorageManager (auto-rotation enabled).
    
    With the new system, this uses automatic file rotation at 512 MB.
    To use the old simple append, call append_listings_to_file_simple().
    
    Args:
        listings: List of listing dictionaries
        storage_manager: FileStorageManager instance (or uses global)
    
    Returns:
        Number of listings written
    """
    if storage_manager is None:
        storage_manager = get_storage_manager()
    
    return storage_manager.append_listings_batch(listings)


def append_listings_to_file_simple(listings, output_file):
    """
    Legacy method: Simple append without rotation.
    
    Use append_listings_to_file() for new code with automatic rotation.
    """
    with open(output_file, "a", encoding="utf-8") as f:
        for listing in listings:
            f.write(json.dumps(listing, separators=(",", ":")) + "\n")


def close_storage_manager():
    """Close and finalize the global storage manager."""
    global _storage_manager
    if _storage_manager is not None:
        _storage_manager.close()


def load_progress(progress_file):
    """Load progress tracking as JSON. Returns dict of completed zip/year keys."""
    if os.path.exists(progress_file):
        try:
            with open(progress_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_progress(progress_dict, progress_file):
    """Save progress tracking as JSON."""
    with open(progress_file, "w") as f:
        json.dump(progress_dict, f, indent=2)


def mark_completed(zip_year_key, progress_file, count=0, error=None):
    """Mark a zip/year combination as completed with metadata."""
    progress = load_progress(progress_file)
    
    progress[zip_year_key] = {
        "completed_at": datetime.now().isoformat(),
        "count": count,
        "error": error
    }
    
    save_progress(progress, progress_file)


def is_processed(zip_year_key, progress_file):
    """Check if a zip/year key has been processed."""
    progress = load_progress(progress_file)
    return zip_year_key in progress


def get_processed_keys(progress_file):
    """Get set of all processed zip/year keys."""
    progress = load_progress(progress_file)
    return set(progress.keys())


def load_zip_codes(filepath):
    """Load zip codes from file."""
    with open(filepath, "r") as f:
        return [line.strip() for line in f if line.strip()]
