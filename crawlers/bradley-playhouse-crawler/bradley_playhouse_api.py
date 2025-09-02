from __future__ import annotations  # Required for modern type hints (list[dict], union types with |)

import logging
import time
from datetime import datetime, timedelta

from curl_cffi import requests           # Lightweight replacement for requests with better performance
from dateutil import parser              # Robust ISO datetime parsing library

from read_config import read_config

# Configuration and proxy setup
config = read_config()
proxy_auth = config.get('PROXY')
proxies = {
    "http": f"http://{proxy_auth}",
    "https": f"http://{proxy_auth}"
}

print(f"Proxy auth: {proxy_auth}")

# Logger configuration for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# API Configuration Constants
BASE_OVT_URL = "https://ci.ovationtix.com/36038/performance"  # Base URL for individual performance pages
CLIENT_ID   = "36038"                                         # The Bradley Playhouse client identifier
CAL_URL     = "https://web.ovationtix.com/trs/api/rest/CalendarProductions"  # Calendar API endpoint
MAX_RETRIES = 3                                               # Maximum number of retry attempts for API calls

# HTTP headers required for OvationTix API authentication and proper request handling
HEADERS     = {
    "Accept"        : "*/*",                    # Accept any content type
    "Content-Type"  : "application/json",      # Specify JSON content type
    "Origin"        : "https://ci.ovationtix.com",     # Required origin header for CORS
    "Referer"       : "https://ci.ovationtix.com/",    # Required referer for API access
    "User-Agent"    : "Mozilla/5.0",           # Browser user agent string
    "clientId"      : CLIENT_ID,               # Client identifier for API authentication
    "newCIRequest"  : "true",                  # Flag indicating new client interface request
}



def _within_range(show_dt: str, start: datetime, end: datetime) -> bool:
    """
    Check if a show datetime string falls within a specified date range.
    Returns:
        bool: True if the parsed datetime is within the range, False otherwise
              Returns False if the datetime string cannot be parsed
    """
    try:
        # Parse the ISO datetime string using dateutil for robust parsing
        dt = parser.isoparse(show_dt)
    except Exception:
        # Return False for any parsing errors (malformed dates, invalid formats, etc.)
        return False

    # Check if the parsed datetime falls within the inclusive range
    return start <= dt <= end


def call_api_with_retries(method, url, headers=None, params=None, data=None):
    """
    Calls an API with automatic retries using exponential backoff strategy.
    """
    delay = 5           # Initial delay in seconds before first retry
    backoff_factor = 2  # Multiplier for exponential backoff (5s, 10s, 20s, etc.)

    # Attempt the request up to MAX_RETRIES times
    for attempt in range(MAX_RETRIES):
        try:
            print(f"[call_api_with_retries] Attempt {attempt+1} for URL: {url}")

            # Execute the appropriate HTTP method
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=30, proxies=proxies)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, data=data, timeout=30, proxies=proxies)
            else:
                # Skip unsupported HTTP methods and continue to next attempt
                print(f"[call_api_with_retries] Unsupported method: {method}")
                continue

            print(f"[call_api_with_retries] Status Code: {response.status_code}")

            # Handle different response status codes
            if response.status_code == 200:
                # Success - return the response immediately
                return response
            elif response.status_code == 404:
                # Not Found - don't retry as this is likely a permanent error
                print(f"[call_api_with_retries] Status {response.status_code}, returning None.")
                return None
            else:
                # Other status codes (5xx server errors, 429 rate limit, etc.) - retry with backoff
                print(f"[call_api_with_retries] Retryable status code: {response.status_code}")
                time.sleep(delay * (backoff_factor ** attempt))

        except Exception as e:
            # Handle network errors, timeouts, and other exceptions
            print(f"[call_api_with_retries] Exception occurred: {e}")
            time.sleep(delay * (backoff_factor ** attempt))

    # All retry attempts exhausted
    print("[call_api_with_retries] All retries failed, returning None.")
    return None


def get_list_of_events(days_forward: int = 730) -> list[dict]:
    """
    Fetch and process upcoming events from the The Bradley Playhouse API.
    """
    # Calculate the date range for event filtering
    # Note: datetime.utcnow() is deprecated but still used for backward compatibility
    now     = datetime.utcnow()  # Current UTC time as the start boundary
    cut_off = now + timedelta(days=days_forward)  # End boundary for event filtering

    # Fetch calendar data from OvationTix API with automatic retry logic
    resp = call_api_with_retries("GET", CAL_URL, headers=HEADERS)

    # Raise an exception if the request failed (resp will be None if all retries failed)
    resp.raise_for_status()
    payload = resp.json()  # Parse the JSON response

    # Initialize list to store processed performance data
    perf: list[dict] = []

    # Process the calendar data structure:
    # payload is a list of days, each containing productions with showtimes
    for day in payload:
        # Each day contains a "productions" array with event information
        for prod in day.get("productions", []):
            # Extract production-level information
            pid        = str(prod.get("productionId", ""))      # Production identifier
            title      = prod.get("name", "").strip()           # Event/show title
            sel_method = (prod.get("seatSelectionMethod") or "").lower()  # Seat selection method

            # Process each showtime/performance within this production
            for show in prod.get("showtimes", []):
                # Get the performance start time in ISO format
                start_iso = show.get("performanceStartTime", "")

                # Skip events outside our date range
                if not _within_range(start_iso, now, cut_off):
                    continue

                # Apply business logic filters to exclude unwanted events
                is_available = bool(show.get("performanceAvailable"))  # Available for purchase
                is_cancelled = bool(show.get("isCancelled"))           # Event cancelled
                is_soldout = bool(show.get("isSoldOut"))               # No tickets available

                # Skip events that are unavailable, cancelled, or sold out
                if not is_available or is_cancelled or is_soldout:
                    continue

                # Parse the datetime for formatting
                dt = parser.isoparse(start_iso)

                # Create standardized event record for downstream processing
                perf.append({
                    "event_id"              : pid,                                      # Production ID
                    "event_unique_id"       : str(show.get("performanceId")),         # Unique performance ID
                    "event_name"            : title,                                   # Event title
                    "event_date"            : dt.date().isoformat(),                  # Date in YYYY-MM-DD format
                    "event_time"            : dt.time().strftime("%H:%M:%S"),         # Time in HH:MM:SS format
                    "seat_selection_method" : sel_method,                             # Seat selection method
                    "event_url"             : f"{BASE_OVT_URL}/{show.get('performanceId')}"  # Direct booking URL
                })

    # Log the results for monitoring and debugging
    logger.info("The Bradley Playhouse → %d performances in next %d days",
                len(perf), days_forward)
    return perf
