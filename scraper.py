"""Core scraping logic for vehicle data."""

import requests
import time
from constants import BASE_URL, MAX_RETRIES, INITIAL_WAIT, API_HEADERS, API_COOKIES


def search_vehicles(zip_code, year_min=None, year_max=None, vehicle_condition="USED", radius=25, rows=25, page=1):
    """
    Search for vehicles in a given zip code with optional filters.
    
    Args:
        zip_code: ZIP code to search
        year_min: Minimum model year
        year_max: Maximum model year
        vehicle_condition: "USED" or "NEW"
        radius: Search radius in miles
        rows: Number of results per page
        page: Page number (1-indexed)
    
    Returns:
        dict: JSON response from API
    
    Raises:
        requests.HTTPError: If request fails after all retries
    """
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
        response = requests.get(BASE_URL, headers=API_HEADERS, cookies=API_COOKIES, params=params)

        if response.status_code == 200:
            return response.json()

        print(f"    ⚠ HTTP {response.status_code} (attempt {attempt}/{MAX_RETRIES}). "
              f"Waiting {wait}s before retry...")
        time.sleep(wait)
        wait *= 2  # exponential backoff: 5, 10, 20, 40, 80, 160...

    # Final attempt failed — raise so the caller can handle it
    response.raise_for_status()


def fetch_all_listings_for_zip(zip_code, year_min, year_max, vehicle_condition, radius=25, rows=25, delay=1.0):
    """
    Fetch all raw listing objects across all pages for a given zip code.
    
    Args:
        zip_code: ZIP code to search
        year_min: Minimum model year
        year_max: Maximum model year
        vehicle_condition: "USED" or "NEW"
        radius: Search radius in miles
        rows: Results per page
        delay: Delay between page requests in seconds
    
    Returns:
        list: All listing objects for the zip/year combination
    """
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
