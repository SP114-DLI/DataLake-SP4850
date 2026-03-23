#!/usr/bin/env python3
"""
Optimized duplicate counter for large JSONL files.

Efficiently detects and counts duplicates across multiple JSONL files using:
- Streaming (no full file load to memory)
- Set-based deduplication with MD5 hashing
- Multiprocessing for parallel file processing
- Progress reporting
"""

import json
import hashlib
import os
import sys
from pathlib import Path
from collections import defaultdict
from multiprocessing import Pool, Manager
from typing import Dict, Tuple, Any
import time


def get_json_hash(obj: dict) -> str:
    """
    Generate a consistent hash for a JSON object.
    Uses MD5 of sorted JSON string for stable hashing.
    """
    try:
        # Convert to JSON with sorted keys for consistency
        json_str = json.dumps(obj, sort_keys=True, separators=(',', ':'))
        return hashlib.md5(json_str.encode()).hexdigest()
    except (TypeError, ValueError):
        # Fallback for non-serializable objects
        return hashlib.md5(str(obj).encode()).hexdigest()


def process_file(args) -> Dict[str, Any]:
    """
    Process a single JSONL file and return statistics about duplicates.
    
    Args:
        args: Tuple of (filepath, file_index, total_files)
    
    Returns:
        Dictionary with:
        - file_path: str
        - total_lines: int
        - duplicate_lines: int
        - unique_hashes: set of hashes (for cross-file comparison)
        - hash_to_lines: dict mapping hash to line numbers
    """
    filepath, file_index, total_files = args
    
    file_hashes = set()
    hash_to_lines = defaultdict(list)
    duplicates_count = 0
    total_count = 0
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                total_count += 1
                
                try:
                    json_obj = json.loads(line.strip())
                    obj_hash = get_json_hash(json_obj)
                    
                    # Track which lines have this hash
                    hash_to_lines[obj_hash].append(line_num)
                    
                    # Count duplicate (if we've seen this hash before in this file)
                    if obj_hash in file_hashes:
                        duplicates_count += 1
                    else:
                        file_hashes.add(obj_hash)
                
                except json.JSONDecodeError as e:
                    print(f"⚠️  Skipping invalid JSON at {filepath}:{line_num}")
                    continue
                
                # Progress indicator every 10k lines
                if line_num % 10000 == 0:
                    print(f"[{file_index}/{total_files}] {filepath.name}: {line_num:,} lines processed")
    
    except Exception as e:
        print(f"❌ Error processing {filepath}: {e}")
        return {
            'file_path': str(filepath),
            'total_lines': 0,
            'duplicate_lines': 0,
            'unique_hashes': set(),
            'hash_to_lines': {}
        }
    
    print(f"✓ Completed {filepath.name}: {total_count:,} lines, {duplicates_count:,} duplicates within file")
    
    return {
        'file_path': str(filepath),
        'total_lines': total_count,
        'duplicate_lines': duplicates_count,
        'unique_hashes': file_hashes,
        'hash_to_lines': dict(hash_to_lines)
    }


def count_duplicates(data_dir: str = 'data', num_workers: int = None) -> None:
    """
    Count duplicates across all JSONL files in the data directory.
    
    Args:
        data_dir: Directory containing JSONL files (default: 'data')
        num_workers: Number of worker processes (default: CPU count)
    """
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"❌ Error: Directory '{data_dir}' not found")
        sys.exit(1)
    
    # Find all JSONL files
    jsonl_files = sorted(data_path.glob('listings_part_*.jsonl'))
    
    if not jsonl_files:
        print(f"⚠️  No JSONL files found in '{data_dir}'")
        sys.exit(1)
    
    print(f"📊 Found {len(jsonl_files)} JSONL files to process")
    print(f"📁 Files: {[f.name for f in jsonl_files[:3]]}{'...' if len(jsonl_files) > 3 else ''}\n")
    
    # Prepare arguments for multiprocessing
    process_args = [(f, i + 1, len(jsonl_files)) for i, f in enumerate(jsonl_files)]
    
    # Determine optimal worker count
    if num_workers is None:
        num_workers = min(os.cpu_count() or 1, len(jsonl_files))
    
    print(f"🔄 Processing with {num_workers} workers...\n")
    
    # Process files in parallel
    start_time = time.time()
    all_hashes = set()
    cross_file_duplicates = defaultdict(list)
    
    with Pool(processes=num_workers) as pool:
        results = pool.map(process_file, process_args)
    
    # Aggregate results for cross-file duplicate detection
    print("\n" + "=" * 70)
    print("Analyzing cross-file duplicates...\n")
    
    total_lines_all = 0
    total_duplicates_within_files = 0
    
    for result in results:
        total_lines_all += result['total_lines']
        total_duplicates_within_files += result['duplicate_lines']
        
        # Track duplicates across files
        for obj_hash, line_nums in result['hash_to_lines'].items():
            if obj_hash in all_hashes:
                cross_file_duplicates[obj_hash].append(result['file_path'])
            else:
                all_hashes.add(obj_hash)
                if len(line_nums) > 1:
                    cross_file_duplicates[obj_hash] = [result['file_path']]
    
    cross_file_dup_count = len(cross_file_duplicates)
    
    # Print comprehensive results
    print("=" * 70)
    print("📈 DUPLICATE ANALYSIS RESULTS")
    print("=" * 70)
    print(f"\n✓ Total Lines Processed:        {total_lines_all:,}")
    print(f"✓ Unique Records (by hash):    {len(all_hashes):,}")
    print(f"✓ Duplicates Within Files:     {total_duplicates_within_files:,}")
    print(f"✓ Duplicates Across Files:     {cross_file_dup_count:,}")
    print(f"✓ Total Duplicate Instances:   {total_lines_all - len(all_hashes):,}")
    print(f"\n📊 Deduplication Ratio:        {(1 - len(all_hashes)/total_lines_all)*100:.2f}%")
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  Processing time:            {elapsed:.2f} seconds")
    print(f"📈 Throughput:                 {total_lines_all/elapsed:,.0f} lines/second")
    
    # Show sample of cross-file duplicates
    if cross_file_dup_count > 0:
        print(f"\n🔄 Cross-File Duplicates (samples):")
        samples = sorted(cross_file_duplicates.items(), 
                        key=lambda x: len(x[1]), 
                        reverse=True)[:5]
        for obj_hash, files in samples:
            print(f"   Hash {obj_hash[:8]}... appears in {len(files)} files")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Count duplicates in JSONL files'
    )
    parser.add_argument(
        '--data-dir',
        default='data',
        help='Directory containing JSONL files (default: data)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of worker processes (default: auto)'
    )
    
    args = parser.parse_args()
    count_duplicates(args.data_dir, args.workers)
