from datetime import datetime
from dateutil import parser
from typing import List, Dict
import requests
import os
import json
import time

# Base URL for Gold Strike ticketing API
API_ENDPOINT_PREFIX = "https://tickets.goldstrike.com"

# Maximum number of retry attempts for API requests
MAX_RETRIES = 3

# Configure proxy authentication if available in environment
proxy_auth = os.getenv("PROXY")
proxies = {
    "http": f"http://{proxy_auth}",
    "https": f"http://{proxy_auth}"
}


def _iso(dt: datetime) -> str:
    """
    Converts a datetime object to ISO date format (YYYY-MM-DD).
    
    Args:
        dt (datetime): The datetime object to format.

    Returns:
        str: ISO formatted date string.
    """
    return dt.strftime("%Y-%m-%d")


def call_api_with_retries(method, url, headers=None, params=None, data=None):
    """
    Calls an API with automatic retries using exponential backoff.

    Args:
        method (str): HTTP method, e.g., 'GET' or 'POST'.
        url (str): API endpoint to call.
        headers (dict, optional): HTTP headers.
        params (dict, optional): Query parameters for GET requests.
        data (dict, optional): Request body for POST requests.

    Returns:
        Response | None: The requests.Response object on success, or None on failure.
    """
    delay = 5  # Initial delay in seconds
    backoff_factor = 2  # Multiplier for exponential backoff

    for attempt in range(MAX_RETRIES):
        try:
            print(f"[call_api_with_retries] Attempt {attempt+1} for URL: {url}")

            # Perform the request based on method
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=30)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, data=data, proxies=proxies, timeout=30)
            else:
                print(f"[call_api_with_retries] Unsupported method: {method}")
                continue

            print(f"[call_api_with_retries] Status Code: {response.status_code}")

            # Successful request
            if response.status_code == 200:
                return response
            # Do not retry on  404
            elif response.status_code in (404):
                print(f"[call_api_with_retries] Status {response.status_code}, returning None.")
                return None
            else:
                # Retry for other status codes
                print(f"[call_api_with_retries] Retryable status code: {response.status_code}")
                time.sleep(delay * (backoff_factor ** attempt))

        except Exception as e:
            print(f"[call_api_with_retries] Exception occurred: {e}")
            time.sleep(delay * (backoff_factor ** attempt))

    print("[call_api_with_retries] All retries failed, returning None.")
    return None


def get_list_of_events(start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches and returns a list of available events between two dates from Gold Strike's ticket widget.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        List[Dict]: List of dictionaries, each representing an available event.
    """
    print(f"[get_list_of_events] Fetching events from {start_date} to {end_date}")
    events_url = f"{API_ENDPOINT_PREFIX}/include/widgets/events/performancelist.asp"

    # Set request headers to mimic a real browser
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "user-agent": "Mozilla/5.0"
    }

    # Define query parameters for the event listing
    params = {
        "fromDate": start_date,
        "toDate": end_date,
        "venue": "0",
        "category": "0",
        "action": "perf",
        "listPageSize": "500",
        "page": "1",
        "cp": "0"
    }

    # Make the API call with retries
    response = call_api_with_retries(method='GET', url=events_url, headers=headers, params=params)

    if not response:
        print("[get_list_of_events] No response received or failed after retries.")
        return []

    # Parse JSON response safely
    try:
        events_data = json.loads(response.text)
        print(f"[get_list_of_events] Events data loaded successfully.")
    except Exception as e:
        print(f"[get_list_of_events] Failed to parse response: {e}")
        return []

    # Extract the list of performances from the response
    performances = events_data.get("performance", [])
    print(f"[get_list_of_events] Found {len(performances)} performances.")

    results = []

    for event in performances:
        try:
            performance_id = event.get("PerformanceID", "")
            event_name = event.get("PerformanceName", "").strip()
            dt_str = event.get("PerformanceDateTime", "")
            dt = parser.parse(dt_str)  # Convert datetime string to datetime object
            event_url = f"{API_ENDPOINT_PREFIX}/orderticketsvenue.asp?p={performance_id}"

            # Extract event flags
            is_available = event.get("IsSalesPeriodStarted", 0) == 1
            is_cancelled = "cancelled" in event.get("SaleIcon", "").lower()
            is_soldout = "sold out" in event.get("SaleIcon", "").lower()

            # Skip events that are not available or already cancelled/sold out
            if not is_available or is_cancelled or is_soldout:
                print(f"[get_list_of_events] Skipping event: {event_name} at {dt} due to unavailability or cancellation.")
                continue

            # Build the event object
            result = {
                "event_id": event.get("EventID", ""),
                "show_id": performance_id,
                "event_name": event_name,
                "event_date": _iso(dt),
                "event_time": dt.strftime("%H:%M:%S"),
                "event_url": event_url,
                "is_available": is_available
            }

            print(f"[get_list_of_events] Processed event: {result['event_name']} at {result['event_date']}")

            results.append(result)

        except Exception as e:
            print(f"[get_list_of_events] Error processing event: {e}")
            continue  # Skip this event and proceed to the next

    print(f"[get_list_of_events] Total events processed: {len(results)}")
    return results
