import requests
import time
import random
import json
import os
from datetime import datetime

from user_config import COOKIES, HEADERS
COOKIES = COOKIES
HEADERS = HEADERS

BASE_URL = "https://helix.carfax.com/search/v2/vehicles"

MAX_RETRIES = 6
INITIAL_WAIT = 5  # seconds


def search_vehicles(zip_code, year_min=None, year_max=None, vehicle_condition="USED", radius=25, rows=25, page=1):
    params = {
        "zip": zip_code,
        "radius": radius,
        "sort": "LOCATION_NEAREST",
        "vehicleCondition": vehicle_condition,
        "rows": rows,
        "dynamicRadius": "false",
        "page": page,
    }
    
    if year_min is not None:
        params["yearMin"] = year_min
    if year_max is not None:
        params["yearMax"] = year_max
    
    wait = INITIAL_WAIT
    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.get(BASE_URL, headers=HEADERS, cookies=COOKIES, params=params)

        if response.status_code == 200:
            return response.json()

        print(f"    ⚠ HTTP {response.status_code} (attempt {attempt}/{MAX_RETRIES}). "
              f"Waiting {wait}s before retry...")
        time.sleep(wait)
        wait *= 2  # exponential backoff: 5, 10, 20, 40, 80, 160...

    # Final attempt failed — raise so the caller can handle it
    response.raise_for_status()


def fetch_all_listings_for_zip(zip_code, year_min, year_max, vehicle_condition, radius=25, rows=25, delay=1.0):
    """Fetch all raw listing objects across all pages for a given zip code."""
    all_listings = []

    data = search_vehicles(zip_code, year_min, year_max, vehicle_condition, radius, rows, page=1)
    total_pages = data.get("totalPageCount", 1)
    total_count = data.get("totalListingCount", 0)

    print(f"  Zip {zip_code}: {total_count} total listings across {total_pages} pages")

    listings = data.get("listings", [])
    all_listings.extend(listings)

    for page in range(2, total_pages + 1):
        time.sleep(delay)
        try:
            data = search_vehicles(zip_code, year_min, year_max, vehicle_condition, radius, rows, page=page)
            listings = data.get("listings", [])
            all_listings.extend(listings)
            print(f"    Page {page}/{total_pages}: {len(listings)} listings")
        except Exception as e:
            print(f"    Error on page {page}: {e}")

    return all_listings


def append_listings_to_file(listings, output_file):
    """Append each listing as a single collapsed JSON line to the output file."""
    with open(output_file, "a", encoding="utf-8") as f:
        for listing in listings:
            f.write(json.dumps(listing, separators=(",", ":")) + "\n")


def load_processed_zips(progress_file):
    """Load set of already-processed zip/year keys."""
    if os.path.exists(progress_file):
        with open(progress_file, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_processed_zip(zip_year_key, progress_file):
    """Append a completed zip/year key to the progress tracker."""
    with open(progress_file, "a") as f:
        f.write(zip_year_key + "\n")


def load_zip_codes(filepath):
    """Load zip codes from file."""
    with open(filepath, "r") as f:
        return [line.strip() for line in f if line.strip()]


def scrape_all(zip_file, output_file, progress_file, year_start, year_end,
               vehicle_condition="USED", batch_year_end=2005, radius=25, delay=1.0):
    """Main scraping loop. Dumps raw listings one-per-line after completing each zip/year combo.

    Args:
        zip_file: Path to file with one zip code per line.
        output_file: Path to output JSONL file (one listing per line).
        progress_file: Path to file tracking completed zip/year keys.
        year_start: First model year to scrape.
        year_end: Last model year to scrape.
        vehicle_condition: "USED" or "NEW".
        batch_year_end: Years <= this are fetched as a single range; after are individual.
        radius: Search radius in miles.
        delay: Seconds between page requests.
    """
    zip_codes = load_zip_codes(zip_file)
    processed = load_processed_zips(progress_file)

    # Build year ranges
    year_ranges = []
    if year_start <= batch_year_end:
        year_ranges.append((year_start, min(batch_year_end, year_end)))
    for year in range(max(batch_year_end + 1, year_start), year_end + 1):
        year_ranges.append((year, year))

    total_combos = len(zip_codes) * len(year_ranges)
    combo_count = 0

    print(f"Zip codes: {len(zip_codes)}, Year ranges: {len(year_ranges)}, Total combos: {total_combos}")
    print(f"Already processed: {len(processed)}")
    print(f"Output: {output_file}")
    print("-" * 60)

    start_time = datetime.now()

    for i, zip_code in enumerate(zip_codes, 1):
        zip_listings = []  # Accumulate all listings for this zip code
        zip_keys_completed = []  # Track keys completed for this zip
        all_skipped = True

        for year_min, year_max in year_ranges:
            zip_year_key = f"{zip_code}_{year_min}-{year_max}"

            if zip_year_key in processed:
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

                zip_keys_completed.append(zip_year_key)

            except Exception as e:
                print(f"    ERROR: {e}")

            time.sleep(delay)

        # Batch write all listings for this zip code at once
        if zip_listings:
            append_listings_to_file(zip_listings, output_file)
            print(f"  >> Saved {len(zip_listings)} total listings for zip {zip_code}")

        # Mark all year ranges as done for this zip
        for key in zip_keys_completed:
            save_processed_zip(key, progress_file)
            processed.add(key)

        if not all_skipped:
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"  >> Zip {i}/{len(zip_codes)} complete. Elapsed: {elapsed:.0f}s")

    print("\n" + "=" * 60)
    print(f"DONE. Total time: {(datetime.now() - start_time).total_seconds():.0f}s")
    print(f"Output file: {output_file}")


if __name__ == "__main__":
    scrape_all(
        zip_file="zip_codes.txt",
        output_file="listings.jsonl",
        progress_file="progress.txt",
        year_start=1982,
        year_end=2026,
        vehicle_condition="USED",
        batch_year_end=2010,
        radius=25,
        delay=0,
    )