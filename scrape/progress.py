"""Progress tracking and zip code loading for scrape jobs."""

import json
import os
from datetime import datetime


def load_progress(progress_file):
    """Load progress tracking dict from JSON file."""
    if os.path.exists(progress_file):
        try:
            with open(progress_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_progress(progress_dict, progress_file):
    """Save progress tracking dict to JSON file."""
    with open(progress_file, "w") as f:
        json.dump(progress_dict, f, indent=2)


def mark_completed(zip_year_key, progress_file, count=0, error=None):
    """Mark a zip/year combination as completed with metadata."""
    progress = load_progress(progress_file)
    progress[zip_year_key] = {
        "completed_at": datetime.now().isoformat(),
        "count": count,
        "error": error,
    }
    save_progress(progress, progress_file)


def is_processed(zip_year_key, progress_file):
    """Check if a zip/year key has been processed."""
    return zip_year_key in load_progress(progress_file)


def get_processed_keys(progress_file):
    """Get set of all processed zip/year keys."""
    return set(load_progress(progress_file).keys())


def load_zip_codes(filepath):
    """Load zip codes from a text file (one per line)."""
    with open(filepath, "r") as f:
        return [line.strip() for line in f if line.strip()]
