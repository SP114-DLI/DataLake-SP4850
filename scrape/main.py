"""Main scraping orchestration."""

import time
from datetime import datetime

from scrape.api import fetch_all_listings_for_zip
from scrape.progress import load_zip_codes, mark_completed, is_processed, get_processed_keys
from raw.storage import FileStorageManager


def scrape_all(
    zip_file,
    output_dir="data",
    output_base="listings",
    progress_file="progress.json",
    year_start=1982,
    year_end=2026,
    vehicle_condition="USED",
    batch_year_end=2005,
    radius=25,
    delay=1.0,
):
    """
    Main scraping loop with automatic file rotation at 512 MB.

    Fetches listings for all zip/year combinations and saves to JSONL files.
    Progress is tracked per zip/year key to allow resuming interrupted runs.
    """
    storage = FileStorageManager(output_dir, output_base)
    zip_codes = load_zip_codes(zip_file)
    processed = get_processed_keys(progress_file)

    # Build year ranges: batch for years <= batch_year_end, individual after
    year_ranges = []
    if year_start <= batch_year_end:
        year_ranges.append((year_start, min(batch_year_end, year_end)))
    for year in range(max(batch_year_end + 1, year_start), year_end + 1):
        year_ranges.append((year, year))

    total_combos = len(zip_codes) * len(year_ranges)
    print(f"Zip codes: {len(zip_codes)}, Year ranges: {len(year_ranges)}, Total combos: {total_combos}")
    print(f"Already processed: {len(processed)}")
    print(f"Output: {output_dir}/{output_base}_part_*.jsonl")
    print(f"File rotation: 512 MB")
    print("-" * 60)

    start_time = datetime.now()

    try:
        for i, zip_code in enumerate(zip_codes, 1):
            zip_listings = []
            zip_keys_completed = []
            all_skipped = True

            for year_min, year_max in year_ranges:
                key = f"{zip_code}_{year_min}-{year_max}"
                if is_processed(key, progress_file):
                    continue

                all_skipped = False
                label = f"{year_min}-{year_max}" if year_min != year_max else str(year_min)
                print(f"[Zip {i}/{len(zip_codes)}] {zip_code}, Years {label}")

                try:
                    listings = fetch_all_listings_for_zip(
                        zip_code, year_min, year_max, vehicle_condition, radius, delay=delay
                    )
                    zip_listings.extend(listings or [])
                    zip_keys_completed.append((key, len(listings) if listings else 0, None))
                except Exception as e:
                    print(f"    ERROR: {e}")
                    zip_keys_completed.append((key, 0, str(e)))

                time.sleep(delay)

            # Batch write all listings for this zip code
            if zip_listings:
                count = storage.append_listings_batch(zip_listings)
                info = storage.get_file_info()
                print(f"  >> Saved {count} listings for zip {zip_code} | "
                      f"File: {info['filename']} ({info['size_mb']:.1f} MB)")

            # Mark all year ranges complete for this zip
            for key, count, error in zip_keys_completed:
                mark_completed(key, progress_file, count=count, error=error)
                processed.add(key)

            if not all_skipped:
                elapsed = (datetime.now() - start_time).total_seconds()
                print(f"  >> Zip {i}/{len(zip_codes)} complete. Elapsed: {elapsed:.0f}s")

    finally:
        storage.close()

    # Summary
    summary = storage.get_manifest_summary()
    print("\n" + "=" * 60)
    print(f"DONE. Total time: {(datetime.now() - start_time).total_seconds():.0f}s")
    print(f"  Files: {summary['total_files']}, Rows: {summary['total_rows']}, "
          f"Size: {summary['total_size_mb']:.1f} MB")


if __name__ == "__main__":
    scrape_all(
        zip_file="zip_codes.txt",
        output_dir="data",
        output_base="listings",
        progress_file="progress.json",
        year_start=1982,
        year_end=2026,
        vehicle_condition="USED",
        batch_year_end=2010,
        radius=25,
        delay=0,
    )
