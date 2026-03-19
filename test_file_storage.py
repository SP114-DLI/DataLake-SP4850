"""Test the file_storage module."""

import json
import os
import tempfile
from pathlib import Path
from file_storage import FileStorageManager


def test_file_storage():
    """Test file rotation and metadata generation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Testing in: {tmpdir}\n")
        
        # Create storage manager with small limit for testing (1 MB instead of 512)
        manager = FileStorageManager(output_dir=tmpdir, base_filename="test_data", manifest_file="manifest.json")
        manager.FILE_SIZE_LIMIT = 1 * 1024 * 1024  # 1 MB for testing
        
        # Generate test listings
        test_listings = []
        for i in range(1000):
            test_listings.append({
                "id": i,
                "title": f"Test Listing {i}" * 20,  # Make it big enough to trigger rotation
                "price": 15000 + i * 100,
                "year": 2020 + (i % 5),
                "make": "Toyota" if i % 2 == 0 else "Honda",
                "model": "Camry" if i % 2 == 0 else "Civic"
            })
        
        print(f"Writing {len(test_listings)} test listings...")
        count = manager.append_listings_batch(test_listings)
        print(f"✓ Wrote {count} listings\n")
        
        # Close to finalize
        print("Finalizing files...")
        manager.close()
        
        # Check manifest
        manifest_path = Path(tmpdir) / "manifest.json"
        assert manifest_path.exists(), "Manifest file not created"
        
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)
        
        print(f"✓ Manifest created with {len(manifest['files'])} files\n")
        
        # Print file details
        print("Files created:")
        total_size = 0
        for file_info in manifest['files']:
            size_mb = file_info['size_bytes'] / (1024 * 1024)
            print(f"  - {file_info['filename']}")
            print(f"      Size: {size_mb:.1f} MB ({file_info['size_bytes']} bytes)")
            print(f"      Rows: {file_info['row_count']}")
            print(f"      MD5: {file_info['md5_hash']}")
            print(f"      Complete: {file_info['is_complete']}")
            total_size += file_info['size_bytes']
        
        # Get summary
        summary = manager.get_manifest_summary()
        print(f"\nSummary:")
        print(f"  Total files: {summary['total_files']}")
        print(f"  Total rows: {summary['total_rows']}")
        print(f"  Total size: {summary['total_size_mb']:.1f} MB")
        print(f"  Average file: {summary['average_file_size_mb']:.1f} MB")
        
        # Verify files exist
        for file_info in manifest['files']:
            file_path = Path(tmpdir) / file_info['filename']
            assert file_path.exists(), f"File not found: {file_path}"
            actual_size = file_path.stat().st_size
            assert actual_size == file_info['size_bytes'], f"Size mismatch for {file_path}"
        
        print(f"\n✓ All {len(manifest['files'])} files verified!")
        print("✓ File storage test PASSED")


if __name__ == "__main__":
    test_file_storage()
