from __future__ import annotations  # Required for modern type hints (list[dict], union types with |)
import logging
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import time
import re
import csv
import os
from datetime import datetime
from dateutil import parser
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

# Logger configuration for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

VENUE_URL = "https://hawaiitheatre.my.salesforce-sites.com/ticket/#"
MAX_RETRIES = 3                                               # Maximum number of retry attempts for API calls

def call_api_with_retries(method, url, headers=None, params=None, data=None):
    """
    Calls an API with automatic retries using exponential backoff strategy.
    """
    delay = 5           # Initial delay in seconds before first retry
    backoff_factor = 2  # Multiplier for exponential backoff (5s, 10s, 20s, etc.)

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"[call_api_with_retries] Attempt {attempt+1} for URL: {url}")

            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=30, proxies=proxies)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=30, proxies=proxies)
            else:
                logger.error(f"[call_api_with_retries] Unsupported method: {method}")
                return None

            logger.info(f"[call_api_with_retries] Status Code: {response.status_code}")

            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                logger.error(f"[call_api_with_retries] Status {response.status_code}, returning None.")
                return None
            else:
                logger.warning(f"[call_api_with_retries] Retryable status {response.status_code}, retrying...")
                time.sleep(delay * (backoff_factor ** attempt))

        except Exception as e:
            logger.error(f"[call_api_with_retries] Exception: {e}")
            time.sleep(delay * (backoff_factor ** attempt))

    logger.error("[call_api_with_retries] All retries failed, returning None.")
    return None

def get_auth_tokens(url, venue):
    data = {}
    try:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
        }

        response = call_api_with_retries("GET", url, headers=headers)
        if not response or response.status_code != 200:
            logger.error(f"[{venue}] Failed to fetch auth tokens.")
            return data

        match = re.search(
            r"Visualforce\.remoting\.Manager\.add\(new \$VFRM\.RemotingProviderImpl\((.*?)\)\);",
            response.text,
            re.DOTALL,
        )
        if not match:
            logger.error(f"[{venue}] No valid token data found in page response.")
            return data

        json_data = json.loads(match.group(1).strip())

        vid = json_data.get("vf", {}).get("vid")
        if vid:
            data["vid"] = vid

        actions = json_data.get("actions", {})
        for action in actions.values():
            for method in action.get("ms", []):
                if method["name"] == "fetchEvents":
                    data["fetchEvents"] = method
                elif method["name"] == "fetchEventDescriptor":
                    data["fetchEventDescriptor"] = method

    except Exception as e:
        logger.error(f"[{venue}] Error while fetching auth tokens: {e}")

    return data

def get_events(venue):
    
    events_data = []
    try:
        domain_match = re.search(r"(https?://[A-Za-z_0-9.-]+)", VENUE_URL)
        domain = domain_match.group(1) if domain_match else "unknown"
        if not domain.startswith("https://"):
            domain = "https://" + domain
        logger.info(f"[{venue}] Using domain: {domain}")

        tokens = get_auth_tokens(VENUE_URL, venue)
        if not tokens:
            logger.error(f"[{venue}] No tokens retrieved, skipping events fetch.")
            return events_data

        url = f"{domain}/ticket/apexremote"

        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": f"{domain}",
            "Referer": f"{domain}/ticket",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "X-User-Agent": "Visualforce-Remoting",
        }

        payload = {
            "action": "PatronTicket.Controller_PublicTicketApp",
            "method": "fetchEvents",
            "data": [f"{domain}/ticket/", "", ""],
            "type": "rpc",
            "tid": 5,
            "ctx": {
                "csrf": tokens.get("fetchEvents", {}).get("csrf", ""),
                "vid": tokens.get("vid", ""),
                "ns": tokens.get("fetchEvents", {}).get("ns", ""),
                "ver": tokens.get("fetchEvents", {}).get("ver", ""),
                "authorization": tokens.get("fetchEvents", {}).get("authorization", "")
            },
        }

        response = call_api_with_retries("POST", url, headers=headers, data=payload)

        if not response:
            logger.error(f"[{venue}] fetchEvents request failed after retries.")
            return events_data
        response_data = response.json()
        status_code = response_data[0].get("statusCode", 0)
        if status_code != 200:
            logger.error(f"[{venue}] fetchEvents failed with status {status_code}")
            return events_data

        results = response_data[0].get("result", [])
        for result in results:
            for event in result.get("instances", []):
                try:
                    if event["saleStatus"] == "Not on sale yet":
                        continue
                    if event["saleStatus"] == "No longer on sale":
                        continue
                    if event["soldOut"] == "True":
                        continue
                    event_id = event["id"]
                    event_name = event["eventName"]
                    is_soldout = event["soldOut"]
                    sale_status = event["saleStatus"]
                    event_url = event["purchaseUrl"]
                    seating_type = event["seatingType"]

                    event_time_un = event["formattedDates"].get("TIME_STRING")  # e.g. "07:30 PM"
                    event_date_un = event["formattedDates"].get("LONG_MONTH_DAY_YEAR")  # e.g. "August 20, 2025"
                    dt_object = datetime.strptime(f"{event_date_un} {event_time_un}", "%B %d, %Y %I:%M %p")
                    event_date = dt_object.strftime("%Y-%m-%d")   # YYYY-MM-DD
                    event_time = dt_object.strftime("%H:%M:%S")
                    events_data.append({
                        "event_id": event_id,
                        "event_name": event_name,
                        "event_date": event_date,
                        "event_time": event_time,
                        # "seating_type": seating_type,
                        "event_url": event_url
                    })
                except Exception as e:
                    logger.error(f"[{venue}] Error parsing event: {e}")

    except Exception as e:
        logger.error(f"[{venue}] Error fetching events: {e}")

    return events_data
