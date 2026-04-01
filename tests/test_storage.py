"""Tests for the raw.storage module (FileStorageManager)."""

import json
import tempfile
from pathlib import Path

from raw.storage import FileStorageManager


def test_file_storage():
    """Test file rotation and metadata generation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Testing in: {tmpdir}\n")

        manager = FileStorageManager(
            output_dir=tmpdir, base_filename="test_data",
            manifest_file="manifest.json", file_size_limit=1 * 1024 * 1024,  # 1 MB for testing
        )

        test_listings = [
            {
                "id": i,
                "title": f"Test Listing {i}" * 20,
                "price": 15000 + i * 100,
                "year": 2020 + (i % 5),
                "make": "Toyota" if i % 2 == 0 else "Honda",
                "model": "Camry" if i % 2 == 0 else "Civic",
            }
            for i in range(1000)
        ]

        print(f"Writing {len(test_listings)} test listings...")
        count = manager.append_listings_batch(test_listings)
        print(f"Wrote {count} listings\n")

        manager.close()

        manifest_path = Path(tmpdir) / "manifest.json"
        assert manifest_path.exists(), "Manifest file not created"

        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        print(f"Manifest: {len(manifest['files'])} files\n")
        for fi in manifest["files"]:
            size_mb = fi["size_bytes"] / (1024 * 1024)
            print(f"  {fi['filename']}: {size_mb:.1f} MB, {fi['row_count']} rows, MD5={fi['md5_hash'][:12]}...")

        summary = manager.get_manifest_summary()
        print(f"\nTotal: {summary['total_files']} files, {summary['total_rows']} rows, "
              f"{summary['total_size_mb']:.1f} MB")

        for fi in manifest["files"]:
            fp = Path(tmpdir) / fi["filename"]
            assert fp.exists(), f"Missing: {fp}"
            assert fp.stat().st_size == fi["size_bytes"], f"Size mismatch: {fp}"

        print("\nAll files verified. PASSED")


if __name__ == "__main__":
    test_file_storage()
