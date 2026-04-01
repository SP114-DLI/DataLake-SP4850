"""Configuration for the Carfax vehicle scraper."""

try:
    from user_config import COOKIES, HEADERS
except ImportError:
    COOKIES = {}
    HEADERS = {}

# Carfax API
BASE_URL = "https://helix.carfax.com/search/v2/vehicles"

# Retry settings (exponential backoff: 5, 10, 20, 40, 80, 160s)
MAX_RETRIES = 6
INITIAL_WAIT = 5  # seconds

# API credentials (loaded from user_config.py at project root)
API_COOKIES = COOKIES
API_HEADERS = HEADERS
