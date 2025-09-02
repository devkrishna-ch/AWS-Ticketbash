import os
import time
import json
import requests
from datetime import datetime
from dateutil import parser

from read_config import read_config

# =====================
# Configuration Values
# =====================
API_ENDPOINT_PREFIX = ""  # Base URL for the venue API
MAX_RETRIES = 3  # Max retries for API calls
venue_url = "https://purchase.americanatheatrebranson.com/"

Process_Fee = 5
Tax = 0.14

# Mapping section names to readable labels
SECTION_MAP_DICT = {
    "reserved seating lower center": "Lower Center",
    "reserved seating lower left": "Lower Left",
    "reserved seating lower right": "Lower Right",
    "reserved seating rear center": "Rear Center",
    "reserved seating rear left": "Rear Left",
    "reserved seating rear right": "Rear Right"
}

# Set proxy from environment variable
cfg = read_config()
proxy_auth = cfg["PROXY"]
proxies = {
    "http": f"http://{proxy_auth}",
    "https": f"http://{proxy_auth}"
}
print("Using proxy:", proxies)

# =====================
# Utility Functions
# =====================

def get_current_timestamp():
    """Return current timestamp in human-readable format."""
    return time.strftime("%d %b %Y %H:%M:%S", time.localtime())


def call_api_with_retries(method, url, headers=None, params=None, data=None):
    """Call API with retries and exponential backoff."""
    delay = 5
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.request(
                method, url, headers=headers, params=params, data=data, proxies=proxies, timeout=30)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                return None
            time.sleep(delay * (2 ** attempt))
        except requests.exceptions.RequestException as e:
            print(f"[API Retry] Attempt {attempt + 1} failed: {e}")
            time.sleep(delay * (2 ** attempt))
    return None


def parse_standardized_dates(timestamp_filter):
    """Convert comma-separated date strings into standardized format."""
    if not timestamp_filter.strip():
        print("No timestamp filter provided.")
        return []
    try:
        return [
            datetime.strptime(d.strip(), "%Y-%m-%d %I:%M:%S %p").strftime("%Y-%m-%d %I:%M:%S %p")
            for d in timestamp_filter.split(",") if d.strip()
        ]
    except Exception as e:
        print(f"Failed to parse timestamp filter: {e}")
        return []


# =====================
# Event and Seat Fetching
# =====================

def get_events(url, start_date, end_date, timestamp_filter, venue):
    """Fetch all events from the venue between start and end dates."""
    events = []
    try:
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "referer": API_ENDPOINT_PREFIX,
            "user-agent": "Mozilla/5.0",
            "x-requested-with": "XMLHttpRequest"
        }
        params = {
            "fromDate": start_date, "toDate": end_date, "venue": "0",
            "category": "0", "action": "perf", "listPageSize": "500",
            "page": "1", "cp": "0"
        }
        url = f"{API_ENDPOINT_PREFIX}/include/widgets/events/performancelist.asp"
        response = call_api_with_retries('GET', url, headers=headers, params=params)

        filter_dates = parse_standardized_dates(timestamp_filter)

        if not response:
            raise Exception("No response from event API.")

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("Invalid JSON from event API.")
            return []

        for evt in data.get("performance", []):
            try:
                perf_id = evt.get("PerformanceID", "")
                date_str = evt.get("PerformanceDateTime", "")
                dt = parser.parse(date_str) if date_str else None
                event_date = dt.strftime("%Y-%m-%d") if dt else ""
                event_time = dt.strftime("%H:%M:%S") if dt else ""
                sdates = dt.strftime("%Y-%m-%d %I:%M:%S %p") if dt else ""

                if filter_dates and sdates not in filter_dates:
                    continue

                if evt.get("InteractiveSeatmapActive") != 1:
                    continue

                if evt.get("ItemType", "").lower() not in ["p", "e"]:
                    continue

                events.append({
                    "event_id": evt.get("EventID", ""),
                    "performance_id": perf_id,
                    "event_name": evt.get("PerformanceName", ""),
                    "event_date": event_date,
                    "event_time": event_time,
                    "event_url": f"{API_ENDPOINT_PREFIX}/orderticketsvenue.asp?p={perf_id}",
                    "event_on_sale": evt.get("IsSalesPeriodStarted", 0) == 1,
                    "event_on_sale_date": evt.get("SalesStart", ""),
                    "event_sale_status": evt.get("SaleIcon", "")
                })
            except Exception as e:
                print(f"Error processing event: {e}")

    except Exception as e:
        print(f"Failed to fetch events: {e}")
    return events


def fetch_json(url, params=None):
    """Helper to fetch JSON via GET request."""
    headers = {"accept": "*/*", "user-agent": "Mozilla/5.0"}
    try:
        response = call_api_with_retries("GET", url, headers=headers, params=params)
        return response.json() if response else {}
    except Exception as e:
        print(f"Failed fetching JSON from {url}: {e}")
        return {}


def get_performance_seatmap(event):
    """Fetch seatmap metadata for an event."""
    url = f"{API_ENDPOINT_PREFIX}/include/modules/SeatingChart/Request/getPerformanceSeatmap.asp"
    return fetch_json(url, {"p": event.get("performance_id")})


def get_seatmap_data(event):
    """Fetch raw seat data file for a performance."""
    url = f"{API_ENDPOINT_PREFIX}/data/SeatMapData/{event.get('performance_id')}-seatdata.txt"
    try:
        response = call_api_with_retries("GET", url, headers={"accept": "*/*"})
        return {x.split("|")[0]: x for x in response.json()} if response else {}
    except Exception as e:
        print(f"Error getting seatmap data: {e}")
        return {}


def get_non_available_seats(event):
    """Fetch seats that are already booked/unavailable."""
    ts = int(time.time() * 1000)
    url = f"{API_ENDPOINT_PREFIX}/include/modules/SeatingChart/request/getPerformanceAvailability.asp"
    return fetch_json(url, {"p": event.get("performance_id"), "_": ts})


def get_seats(event, venue):
    """Extract available seats for a given event."""
    all_seats = []
    try:
        seatmap = get_performance_seatmap(event)
        if not seatmap:
            print("No seatmap data")
            return []

        # Build lookup dictionaries
        sections_dict = {s["sectionID"]: s["name"] for s in seatmap.get("sections", [])}
        categories_dict = {c["id"]: c["name"] for c in seatmap.get("categories", [])}
        special_dict = {s["id"]: s["name"] for s in seatmap.get("specialSeating", [])}
        pricing_dict = {}

        # Filter valid prices
        for price in seatmap.get("prices", []):
            if any(kw in price.get("priceCodeName", "").lower() for kw in ['vip', 'premium']):
                continue
            cat = price.get("seatCategory")
            name = categories_dict.get(cat, "").lower()
            if any(kw in name for kw in ["wheelchair", "companion", "obstructed", "accessible", "standing room only","ada seating","vip","obstructed view","partially obstructed view"]):
                continue
            pricing_dict[cat] = max(price.get("price", 0), pricing_dict.get(cat, 0))

        # Fetch seat data and filter
        seats_dict = get_seatmap_data(event)
        non_avl = get_non_available_seats(event)
        available = [seats_dict[s] for s in seats_dict if s not in non_avl]

        for seat_raw in available:
            try:
                parts = seat_raw.split("|")
                if len(parts) < 9:
                    continue

                section_id = int(parts[3]) if parts[3].isdigit() else 0
                section_name = sections_dict.get(section_id, "").lower()
                if any(kw in section_name for kw in ["wheelchair", "companion", "obstructed", "accessible", "standing room only","ada seating","vip","obstructed view","partially obstructed view"]):
                    continue

                special_id = parts[8]
                if special_id and special_id.isdigit():
                    special_name = special_dict.get(int(special_id), "").lower()
                    if any(kw in special_name for kw in ["wheelchair", "companion", "obstructed", "accessible", "standing room only","ada seating","vip","obstructed view","partially obstructed view"]):
                        continue

                category_id = int(parts[4]) if parts[4].isdigit() else 0
                price = pricing_dict.get(category_id, 0)
                if price <= 0:
                    continue

                section_final = SECTION_MAP_DICT.get(section_name, section_name.title())
                row, seat = parts[5], parts[6]
                if not (section_final and row and seat):
                    continue

                all_seats.append({
                    "Venue Name": venue,
                    "Event Name": event["event_name"],
                    "Event Date": event["event_date"],
                    "Event Time": event["event_time"],
                    "Section": section_final,
                    "Row": row,
                    "Seat": seat,
                    "Price": round(price + Process_Fee + (price * Tax) , 2) ,
                    "Desc": "",
                    "UniqueIdentifier": event.get("performance_id"),
                    "TimeStamp": get_current_timestamp()
                })
            except Exception as e:
                print(f"Error parsing seat: {e}")
                continue

        print(f"Seats extracted: {len(all_seats)}")
    except Exception as e:
        print(f"Failed to extract seats: {e}")
    return all_seats


# =====================
# Scraper Entry Point
# =====================

def scrape_event( venue_name, event_id, start_date, end_date, max_retries):
    """Main function to scrape seat data for a given event."""
    global MAX_RETRIES, API_ENDPOINT_PREFIX
    try:
        MAX_RETRIES = int(max_retries or 3)
        API_ENDPOINT_PREFIX = venue_url
        events = get_events(venue_url, start_date, end_date, "", venue_name)
        if not events:
            raise Exception("No events found")

        target = next((e for e in events if str(e.get("performance_id")) == str(event_id)), None)
        if not target:
            raise Exception(f"Event ID {event_id} not found")

        if not target.get("event_on_sale", False):
            raise Exception("Event not on sale")
        if any(x in target.get("event_sale_status", "").lower() for x in ["sold out", "cancelled", "postponed"]):
            raise Exception(f"Event status: {target.get('event_sale_status')}")

        seats = get_seats(target, venue_name)
        if not seats:
            raise Exception("No seats found")

        return json.dumps({
            "status": "success",
            "message": f"Successfully extracted {len(seats)} seats",
            "event_data": seats
        })

    except Exception as e:
        print(f"scrape_event error: {e}")
        return json.dumps({"status": "error", "message": str(e)})

# output = scrape_event("Americana Theatre", "711", "2025-06-20", "2027-05-22", "3")

# # Pretty-print the output line by line
# try:
#     parsed_output = json.loads(output)
#     print(json.dumps(parsed_output, indent=2))
# except Exception as e:
#     print("Failed to parse output as JSON:", e)
#     print(output)
