"""
Duplicate counter for large JSONL files.

Detects and counts duplicates across multiple JSONL files using:
- Streaming (no full file load to memory)
- Set-based deduplication with MD5 hashing
- Multiprocessing for parallel file processing
"""

import json
import hashlib
import os
import sys
import time
import argparse
from pathlib import Path
from collections import defaultdict
from multiprocessing import Pool


def get_json_hash(obj):
    """Generate a consistent MD5 hash for a JSON object (sorted keys)."""
    try:
        json_str = json.dumps(obj, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(json_str.encode()).hexdigest()
    except (TypeError, ValueError):
        return hashlib.md5(str(obj).encode()).hexdigest()


def process_file(args):
    """
    Process a single JSONL file and return duplicate statistics.

    Args:
        args: Tuple of (filepath, file_index, total_files)
    """
    filepath, file_index, total_files = args

    file_hashes = set()
    hash_to_lines = defaultdict(list)
    duplicates_count = 0
    total_count = 0

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                total_count += 1
                try:
                    obj_hash = get_json_hash(json.loads(line.strip()))
                    hash_to_lines[obj_hash].append(line_num)
                    if obj_hash in file_hashes:
                        duplicates_count += 1
                    else:
                        file_hashes.add(obj_hash)
                except json.JSONDecodeError:
                    print(f"  Skipping invalid JSON at {filepath}:{line_num}")

                if line_num % 10000 == 0:
                    print(f"[{file_index}/{total_files}] {filepath.name}: {line_num:,} lines processed")

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return {"file_path": str(filepath), "total_lines": 0, "duplicate_lines": 0,
                "unique_hashes": set(), "hash_to_lines": {}}

    print(f"Completed {filepath.name}: {total_count:,} lines, {duplicates_count:,} duplicates within file")
    return {
        "file_path": str(filepath),
        "total_lines": total_count,
        "duplicate_lines": duplicates_count,
        "unique_hashes": file_hashes,
        "hash_to_lines": dict(hash_to_lines),
    }


def count_duplicates(data_dir="data", num_workers=None):
    """Count duplicates across all JSONL files in the data directory."""
    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"Error: Directory '{data_dir}' not found")
        sys.exit(1)

    jsonl_files = sorted(data_path.glob("listings_part_*.jsonl"))
    if not jsonl_files:
        print(f"No JSONL files found in '{data_dir}'")
        sys.exit(1)

    print(f"Found {len(jsonl_files)} JSONL files to process")
    if num_workers is None:
        num_workers = min(os.cpu_count() or 1, len(jsonl_files))
    print(f"Processing with {num_workers} workers...\n")

    process_args = [(f, i + 1, len(jsonl_files)) for i, f in enumerate(jsonl_files)]

    start_time = time.time()
    all_hashes = set()
    cross_file_duplicates = defaultdict(list)

    with Pool(processes=num_workers) as pool:
        results = pool.map(process_file, process_args)

    # Aggregate results
    print("\n" + "=" * 70)
    print("Analyzing cross-file duplicates...\n")

    total_lines_all = 0
    total_duplicates_within = 0

    for result in results:
        total_lines_all += result["total_lines"]
        total_duplicates_within += result["duplicate_lines"]

        for obj_hash, line_nums in result["hash_to_lines"].items():
            if obj_hash in all_hashes:
                cross_file_duplicates[obj_hash].append(result["file_path"])
            else:
                all_hashes.add(obj_hash)
                if len(line_nums) > 1:
                    cross_file_duplicates[obj_hash] = [result["file_path"]]

    cross_file_dup_count = len(cross_file_duplicates)

    print("=" * 70)
    print("DUPLICATE ANALYSIS RESULTS")
    print("=" * 70)
    print(f"\n  Total Lines:              {total_lines_all:,}")
    print(f"  Unique Records:           {len(all_hashes):,}")
    print(f"  Duplicates Within Files:  {total_duplicates_within:,}")
    print(f"  Duplicates Across Files:  {cross_file_dup_count:,}")
    print(f"  Total Duplicate Instances: {total_lines_all - len(all_hashes):,}")
    print(f"\n  Deduplication Ratio:      {(1 - len(all_hashes) / total_lines_all) * 100:.2f}%")

    elapsed = time.time() - start_time
    print(f"\n  Processing time:          {elapsed:.2f} seconds")
    print(f"  Throughput:               {total_lines_all / elapsed:,.0f} lines/second")

    if cross_file_dup_count > 0:
        print(f"\n  Cross-File Duplicates (top 5):")
        for obj_hash, files in sorted(cross_file_duplicates.items(),
                                       key=lambda x: len(x[1]), reverse=True)[:5]:
            print(f"    Hash {obj_hash[:8]}... in {len(files)} files")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count duplicates in JSONL files")
    parser.add_argument("--data-dir", default="data", help="Directory containing JSONL files")
    parser.add_argument("--workers", type=int, default=None, help="Number of worker processes")
    args = parser.parse_args()
    count_duplicates(args.data_dir, args.workers)
