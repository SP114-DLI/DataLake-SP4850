"""Main scraping orchestration."""

import time
from datetime import datetime

from scraper import fetch_all_listings_for_zip
from file_ops import (
    append_listings_to_file,
    get_storage_manager,
    close_storage_manager,
    load_progress,
    mark_completed,
    is_processed,
    get_processed_keys,
    load_zip_codes,
)


def scrape_all(zip_file, output_dir=".", output_base="listings", progress_file="progress.json", year_start=1982, year_end=2026,
               vehicle_condition="USED", batch_year_end=2005, radius=25, delay=1.0):
    """
    Main scraping orchestration loop with automatic file rotation at 512 MB.
    
    Fetches listings for all zip/year combinations and saves them to output files.
    Files are automatically rotated at 512 MB with metadata tracking for Minio.
    Progress is tracked to allow resuming interrupted runs.
    
    Args:
        zip_file: Path to file with one zip code per line
        output_dir: Directory for output files (default ".")
        output_base: Base name for output files (default "listings")
        progress_file: Path to file tracking completed zip/year keys (default "progress.json")
        year_start: First model year to scrape (default 1982)
        year_end: Last model year to scrape (default 2026)
        vehicle_condition: "USED" or "NEW" (default "USED")
        batch_year_end: Years <= this are fetched as range; after are individual (default 2005)
        radius: Search radius in miles (default 25)
        delay: Seconds between page requests (default 1.0)
    """
    # Initialize file storage manager with automatic rotation
    storage_manager = get_storage_manager(output_dir=output_dir, base_filename=output_base)
    
    zip_codes = load_zip_codes(zip_file)
    processed = get_processed_keys(progress_file)

    # Build year ranges
    year_ranges = []
    if year_start <= batch_year_end:
        year_ranges.append((year_start, min(batch_year_end, year_end)))
    for year in range(max(batch_year_end + 1, year_start), year_end + 1):
        year_ranges.append((year, year))

    total_combos = len(zip_codes) * len(year_ranges)

    print(f"Zip codes: {len(zip_codes)}, Year ranges: {len(year_ranges)}, Total combos: {total_combos}")
    print(f"Already processed: {len(processed)}")
    print(f"Output dir: {output_dir}, Base filename: {output_base}")
    print(f"Progress: {progress_file}")
    print(f"File size limit: 512 MB (auto-rotation enabled)")
    print("-" * 60)

    start_time = datetime.now()

    for i, zip_code in enumerate(zip_codes, 1):
        zip_listings = []  # Accumulate all listings for this zip code
        zip_keys_completed = []  # Track keys completed for this zip
        all_skipped = True

        for year_min, year_max in year_ranges:
            zip_year_key = f"{zip_code}_{year_min}-{year_max}"

            if is_processed(zip_year_key, progress_file):
                continue

            all_skipped = False
            year_label = f"{year_min}-{year_max}" if year_min != year_max else str(year_min)
            print(f"[Zip {i}/{len(zip_codes)}] {zip_code}, Years {year_label}")

            try:
                listings = fetch_all_listings_for_zip(
                    zip_code, year_min, year_max, vehicle_condition, radius, delay=delay
                )

                if listings:
                    zip_listings.extend(listings)
                    zip_keys_completed.append((zip_year_key, len(listings), None))
                else:
                    zip_keys_completed.append((zip_year_key, 0, None))

            except Exception as e:
                print(f"    ERROR: {e}")
                zip_keys_completed.append((zip_year_key, 0, str(e)))

            time.sleep(delay)

        # Batch write all listings for this zip code at once
        if zip_listings:
            count = append_listings_to_file(zip_listings, storage_manager=storage_manager)
            file_info = storage_manager.get_file_info()
            print(f"  >> Saved {count} listings for zip {zip_code} | File: {file_info['filename']} ({file_info['size_mb']:.1f} MB)")

        # Mark all year ranges as done for this zip
        for key, count, error in zip_keys_completed:
            mark_completed(key, progress_file, count=count, error=error)
            processed.add(key)

        if not all_skipped:
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"  >> Zip {i}/{len(zip_codes)} complete. Elapsed: {elapsed:.0f}s")

    # Finalize and close storage manager
    close_storage_manager()
    
    # Print final summary
    manifest_summary = storage_manager.get_manifest_summary()
    print("\n" + "=" * 60)
    print(f"DONE. Total time: {(datetime.now() - start_time).total_seconds():.0f}s")
    print(f"\nFile Storage Summary:")
    print(f"  Total files created: {manifest_summary['total_files']}")
    print(f"  Total rows: {manifest_summary['total_rows']}")
    print(f"  Total size: {manifest_summary['total_size_mb']:.1f} MB")
    print(f"  Average file size: {manifest_summary['average_file_size_mb']:.1f} MB")
    print(f"\nManifest: {output_dir}/file_manifest.json")


if __name__ == "__main__":
    scrape_all(
        zip_file="zip_codes.txt",
        output_dir=".",
        output_base="listings",
        progress_file="progress.json",
        year_start=1982,
        year_end=2026,
        vehicle_condition="USED",
        batch_year_end=2010,
        radius=25,
        delay=0,
    )