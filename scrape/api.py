"""Carfax API client with retry logic and pagination."""

import time

import requests

from scrape.config import BASE_URL, MAX_RETRIES, INITIAL_WAIT, API_HEADERS, API_COOKIES


def search_vehicles(zip_code, year_min=None, year_max=None, vehicle_condition="USED",
                    radius=25, rows=25, page=1):
    """
    Search for vehicles via the Carfax API with exponential backoff retry.

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

        print(f"    HTTP {response.status_code} (attempt {attempt}/{MAX_RETRIES}). Waiting {wait}s...")
        time.sleep(wait)
        wait *= 2

    response.raise_for_status()


def fetch_all_listings_for_zip(zip_code, year_min, year_max, vehicle_condition,
                               radius=25, rows=25, delay=1.0):
    """
    Fetch all listing objects across all pages for a zip/year combination.

    Returns:
        list: All listing dicts for the given zip/year range
    """
    all_listings = []

    data = search_vehicles(zip_code, year_min, year_max, vehicle_condition, radius, rows, page=1)
    total_pages = data.get("totalPageCount", 1)
    total_count = data.get("totalListingCount", 0)

    print(f"  Zip {zip_code}: {total_count} total listings across {total_pages} pages")
    all_listings.extend(data.get("listings", []))

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
