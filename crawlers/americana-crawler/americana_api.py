from datetime import datetime
from dateutil import parser
from typing import List, Dict
import requests
import os
import json
import time

from read_config import read_config 

API_ENDPOINT_PREFIX = "https://purchase.americanatheatrebranson.com/"
MAX_RETRIES = 3

cfg = read_config()

proxy_auth = cfg["PROXY"]
proxies = {
    "http": f"http://{proxy_auth}",
    "https": f"http://{proxy_auth}"
}

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def call_api_with_retries(method, url, headers=None, params=None, data=None):
    delay = 5
    backoff_factor = 2
    for attempt in range(MAX_RETRIES):
        try:
            print(f"[call_api_with_retries] Attempt {attempt+1} for URL: {url}")
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=30)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, data=data, proxies=proxies, timeout=30)
            else:
                print(f"[call_api_with_retries] Unsupported method: {method}")
                continue

            print(f"[call_api_with_retries] Status Code: {response.status_code}")
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                print(f"[call_api_with_retries] Status {response.status_code}, returning None.")
                return None
            else:
                print(f"[call_api_with_retries] Retryable status code: {response.status_code}")
                time.sleep(delay * (backoff_factor ** attempt))
        except Exception as e:
            print(f"[call_api_with_retries] Exception occurred: {e}")
            time.sleep(delay * (backoff_factor ** attempt))
    print("[call_api_with_retries] All retries failed, returning None.")
    return None

def get_list_of_events(start_date: str, end_date: str) -> List[Dict]:
    print(f"[get_list_of_events] Fetching events from {start_date} to {end_date}")
    events_url = f"{API_ENDPOINT_PREFIX}/include/widgets/events/performancelist.asp"

    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "user-agent": "Mozilla/5.0"
    }

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

    response = call_api_with_retries(method='GET', url=events_url, headers=headers, params=params)

    if not response:
        print("[get_list_of_events] No response received or failed after retries.")
        return []

    try:
        events_data = json.loads(response.text)
        print(f"[get_list_of_events] Events data loaded successfully.")
    except Exception as e:
        print(f"[get_list_of_events] Failed to parse response: {e}")
        return []

    performances = events_data.get("performance", [])
    print(f"[get_list_of_events] Found {len(performances)} performances.")

    results = []
    for event in performances:
        try:
            performance_id = event.get("PerformanceID", "")
            event_name = event.get("PerformanceName", "").strip()
            dt_str = event.get("PerformanceDateTime", "")
            dt = parser.parse(dt_str)
            event_url = f"{API_ENDPOINT_PREFIX}/orderticketsvenue.asp?p={performance_id}"

            # Skip cancelled or sold-out events
            sale_icon = event.get("SaleIcon", "").lower()
            if "cancelled" in sale_icon or "sold out" in sale_icon:
                print(f"[get_list_of_events] Skipping cancelled/sold-out event: {event_name}")
                continue

            result = {
                "event_id": event.get("EventID", ""),
                "show_id": performance_id,
                "event_name": event_name,
                "event_date": _iso(dt),
                "event_time": dt.strftime("%H:%M:%S"),
                "event_url": event_url,
                "created_at" : datetime.now().strftime("%Y-%m-%d %H:%M:%S")                
            }

            print(f"[get_list_of_events] Processed event: {result['event_name']} at {result['event_date']}")

            results.append(result)
        except Exception as e:
            print(f"[get_list_of_events] Error processing event: {e}")
            continue

    print(f"[get_list_of_events] Total events processed: {len(results)}")
    return results


# results = get_list_of_events("2025-09-01", "2025-12-01")
# print(json.dumps(results, indent=2))