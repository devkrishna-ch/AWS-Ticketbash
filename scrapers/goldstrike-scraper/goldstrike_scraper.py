from dateutil import parser
from datetime import datetime
import requests
import time
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_ENDPOINT_PREFIX = ""
MAX_RETRIES = 3
REQUEST_DELAY = 0.5

SECTION_MAP_DICT = {
    "reserved seating lower center": "Lower Center",
    "reserved seating lower left": "Lower Left",
    "reserved seating lower right": "Lower Right",
    "reserved seating rear center": "Rear Center",
    "reserved seating rear left": "Rear Left",
    "reserved seating rear right": "Rear Right"
}

venue_url = "https://tickets.goldstrike.com"

# Enhanced session configuration with stealth features
def create_session():
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=2,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,
        pool_maxsize=10
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Enhanced headers to mimic real browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    })

    return session

# Global session instance
SESSION = create_session()

def make_request(method, url, headers=None, params=None, data=None, timeout=45, require_cookies=False):
    """Enhanced request function with stealth features and cookie handling"""
    try:
        print(f"Making {method} request to: {url[:100]}...")

        # Get cookies from main site first if required
        if require_cookies:
            try:
                print("Getting cookies from main site...")
                main_url = f"{API_ENDPOINT_PREFIX}/"
                cookie_headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                SESSION.get(main_url, headers=cookie_headers, timeout=30)
                time.sleep(2)
            except Exception as e:
                print(f"Cookie setup failed: {e}")

        # Add random delay to mimic human behavior
        time.sleep(REQUEST_DELAY + (time.time() % 1))

        # Merge headers with session defaults
        if headers:
            request_headers = SESSION.headers.copy()
            request_headers.update(headers)
        else:
            request_headers = SESSION.headers

        # Add referer for API calls
        if 'include/' in url or 'data/' in url:
            request_headers['Referer'] = API_ENDPOINT_PREFIX + '/'
            request_headers['X-Requested-With'] = 'XMLHttpRequest'

        if method.upper() == 'GET':
            response = SESSION.get(url, headers=request_headers, params=params, timeout=timeout)
        elif method.upper() == 'POST':
            response = SESSION.post(url, headers=request_headers, data=data, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        print(f"Response status: {response.status_code}")

        if response.status_code == 200:
            return response
        elif response.status_code == 404:
            print(f"404 Not Found: {url}")
            return None
        elif response.status_code == 429:
            print(f"429 Rate Limited: {url} - waiting longer")
            time.sleep(10)
            return None
        else:
            print(f"HTTP {response.status_code} for {url}")
            return None

    except requests.exceptions.Timeout as e:
        print(f"Timeout after {timeout}s for {url}: {str(e)}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for {url}: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error for {url}: {str(e)}")
        return None


def get_events(url, start_date, end_date, timestamp_filter, venue):
    """Optimized event fetching with smaller page sizes"""
    events_list = []
    try:
        events_url = f"{API_ENDPOINT_PREFIX}/include/widgets/events/performancelist.asp"

        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "referer": API_ENDPOINT_PREFIX,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        # Optimized parameters - smaller page size for faster response
        params = {
            "fromDate": start_date,
            "toDate": end_date,
            "venue": "0",
            "city": "",
            "swEvent": "",
            "category": "0",
            "searchString": "",
            "searchType": "0",
            "showHidden": "0",
            "showPackages": "0",
            "action": "perf",
            "listPageSize": "100",  # Reduced from 500
            "listMaxSize": "0",
            "page": "1",
            "cp": "0"
        }

        response = make_request(method='GET', url=events_url, headers=headers, params=params,
                                timeout=60, require_cookies=True)

        # Handle timestamp filter more robustly
        standardized_dates = []
        if timestamp_filter and timestamp_filter.strip():
            try:
                list_of_datesTime = timestamp_filter.split(",")
                standardized_dates = [
                    datetime.strptime(date.strip(), "%Y-%m-%d %I:%M:%S %p").strftime("%Y-%m-%d %I:%M:%S %p")
                    for date in list_of_datesTime if date.strip()
                ]
                print(f"Filtering for specific dates: {standardized_dates}")
            except Exception as e:
                print(f"Error parsing timestamp filter: {e}. Proceeding without filter.")
                standardized_dates = []
        else:
            print("No date time stamp filter provided - processing all events")

        if response:
            print("Request to fetch events successful.")

            try:
                events_data = response.json()
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON response: {e}")
                return events_list

            performances = events_data.get("performance", [])
            print(f"Found {len(performances)} performances")

            for event in performances:
                try:
                    performance_id = event.get("PerformanceID", "")
                    event_date_raw = " ".join(event.get("PerformanceDateTime", "").split(" ")[:-2])
                    event_time_raw = " ".join(event.get("PerformanceDateTime", "").split(" ")[-2:])
                    event_name = event.get("PerformanceName", "")
                    
                    # Convert date format from "Friday, October 17, 2025" to "2025-10-17"
                    try:
                        if event_date_raw and "," in event_date_raw:
                            # Remove the day name (e.g., "Friday, ") and parse the rest
                            date_without_day = event_date_raw.split(", ", 1)[1]  # "October 17, 2025"
                            parsed_date = datetime.strptime(date_without_day, "%B %d, %Y")
                            event_date = parsed_date.strftime("%Y-%m-%d")
                            print(f"Converted date: {event_date_raw} -> {event_date}")
                        else:
                            event_date = event_date_raw
                    except ValueError as e:
                        print(f"Date conversion error: {e}")
                        event_date = event_date_raw
                    
                    # Convert time format from "8:00:00 PM" to "20:00:00"
                    try:
                        if event_time_raw and ("AM" in event_time_raw or "PM" in event_time_raw):
                            parsed_time = datetime.strptime(event_time_raw, "%I:%M:%S %p")
                            event_time = parsed_time.strftime("%H:%M:%S")
                        else:
                            event_time = event_time_raw
                    except ValueError:
                        event_time = event_time_raw
                    item_type = event.get("ItemType", "")
                    is_on_sale = True if event.get("IsSalesPeriodStarted", 0) == 1 else False
                    on_sale_start_date = event.get("SalesStart", "")
                    event_url = f"{API_ENDPOINT_PREFIX}/orderticketsvenue.asp?p={performance_id}"

                    if not item_type.lower().strip() in ["p", "e"]:
                        continue

                    # Parse date more safely
                    try:
                        if event_date and event_time:
                            parsed_date = parser.parse(event_date + " " + event_time)
                            sdates = parsed_date.strftime("%Y-%m-%d %I:%M:%S %p")
                        else:
                            sdates = ""
                    except Exception as e:
                        print(f"Date parsing error: {e}")
                        sdates = ""

                    # Apply timestamp filter if provided
                    if standardized_dates:
                        if sdates in standardized_dates:
                            print(f"{sdates} is present. Event {event_name}-{event_date} {event_time}")
                        else:
                            continue

                    is_seatmap_active = True if event.get("InteractiveSeatmapActive") == 1 else False

                    if not is_seatmap_active:
                        print(f"Skipping {event_name} - seatmap not active")
                        continue

                    events_list.append({
                        "event_id": event.get("EventID", ""),
                        "performance_id": performance_id,
                        "event_name": event_name,
                        "event_date": event_date,
                        "event_time": event_time,
                        "event_url": event_url,
                        "event_on_sale": is_on_sale,
                        "event_on_sale_date": on_sale_start_date,
                        "event_sale_status": event.get("SaleIcon", "")
                    })

                except Exception as e:
                    print(f"Error processing event: {e}")
                    continue

        else:
            print("No response from events API")
            raise Exception("No response from events API")

    except Exception as e:
        print(f"Error fetching events: {e}")
        current_time = get_current_timestamp()
        log_event(current_time, url, venue, "NA", "NA", "NA", "NA", f"Error fetching events: {str(e)}")

    print(f"Returning {len(events_list)} events")
    return events_list


def get_performance_seatmap(event):
    """Get seatmap with better error handling"""
    try:
        url = f"{API_ENDPOINT_PREFIX}/include/modules/SeatingChart/Request/getPerformanceSeatmap.asp"
        params = {"p": event.get("performance_id")}
        headers = {
            "accept": "*/*",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        response = make_request('GET', url, headers=headers, params=params, timeout=30)

        if response:
            try:
                return response.json()
            except json.JSONDecodeError:
                print("Failed to parse seatmap JSON")
                return None
        return None

    except Exception as e:
        print(f"Error getting seatmap: {e}")
        return None


def get_seatmap_data(event):
    """Get seat data with optimized error handling"""
    try:
        url = f"{API_ENDPOINT_PREFIX}/data/SeatMapData/{event.get('performance_id')}-seatdata.txt"
        headers = {
            "accept": "*/*",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        response = make_request('GET', url, headers=headers, timeout=30)

        if response:
            try:
                data = response.json()
                return {x.split("|")[0]: x for x in data if "|" in str(x)}
            except (json.JSONDecodeError, AttributeError, IndexError):
                print("Failed to parse seat data")
                return {}
        return {}

    except Exception as e:
        print(f"Error getting seat data: {e}")
        return {}


def get_non_available_seats(event):
    """Get non-available seats with better error handling"""
    try:
        url = f"{API_ENDPOINT_PREFIX}/include/modules/SeatingChart/request/getPerformanceAvailability.asp"
        params = {"p": event.get("performance_id"), "_": int(time.time() * 1000)}
        headers = {
            "accept": "*/*",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        response = make_request('GET', url, headers=headers, params=params, timeout=30)

        if response:
            try:
                data = response.json()
                return {x.split("|")[0]: x for x in data if "|" in str(x)}
            except (json.JSONDecodeError, AttributeError, IndexError):
                print("Failed to parse availability data")
                return {}
        return {}

    except Exception as e:
        print(f"Error getting availability: {e}")
        return {}


def calculate_facility_fee(price):
    """Calculate facility fee based on price tiers"""
    if price < 20: return 6.99
    if price < 25: return 8.99  # Updated: $19.50 shows $8.99 in some cases
    if price < 35: return 9.99  # Updated: $29.50 shows $9.99
    if price < 45: return 10.99
    if price < 55: return 11.99
    return 12.99


def get_seats(event, venue):
    """Optimized seat extraction with better error handling"""
    print(f"Getting seats for event: {event.get('event_name')}")
    all_seats = []

    try:
        # Get seatmap configuration
        seatmap = get_performance_seatmap(event)
        if not seatmap:
            print("No seatmap data available")
            return []

        # Extract configuration data
        sections = {s.get("sectionID"): s.get("name", "") for s in seatmap.get("sections", [])}
        categories = {c.get("id"): c.get("name", "") for c in seatmap.get("categories", [])}
        specials = {s.get("id"): s.get("name", "") for s in seatmap.get("specialSeating", [])}

        # Build pricing map
        pricing = {}
        for p in seatmap.get("prices", []):
            name = p.get("priceCodeName", "").lower()
            if any(kw in name for kw in ['vip', 'premium']):
                continue
            cat = p.get("seatCategory")
            cat_name = categories.get(cat, "").lower()
            if any(kw in cat_name for kw in ["wheelchair", "companion", "obstructed", "accessible", "standing room only", "ada seating", "vip", "obstructed view", "partially obstructed view"]):
                continue
            pricing[cat] = max(p.get("price", 0), pricing.get(cat, 0))

        print(f"Found {len(pricing)} price categories")

        # Get seat data and availability
        seats = get_seatmap_data(event)
        non_avail = get_non_available_seats(event)

        print(f"Processing {len(seats)} seats, {len(non_avail)} unavailable")

        # Process seats efficiently
        for sid, data in seats.items():
            if sid in non_avail:
                continue

            try:
                info = data.split("|")
                if len(info) < 9:
                    continue

                cat_id = int(info[4]) if info[4].isdigit() else 0
                price = pricing.get(cat_id, 0)
                if price <= 0:
                    continue

                section_id = int(info[3]) if info[3].isdigit() else 0
                section_name = sections.get(section_id, "").lower().strip()
                if any(kw in section_name for kw in ["wheelchair", "companion", "obstructed", "accessible", "standing room only", "ada seating", "vip", "obstructed view", "partially obstructed view"]):
                    continue

                special_id = info[8]
                if special_id.isdigit():
                    special_name = specials.get(int(special_id), "").lower()
                    if any(kw in special_name for kw in ["wheelchair", "companion", "obstructed", "accessible", "standing room only", "ada seating", "vip", "obstructed view", "partially obstructed view"]):
                        continue

                section = SECTION_MAP_DICT.get(section_name, section_name.title())
                row, seat = info[5], info[6]

                if not (section and row and seat):
                    continue

                # Calculate totals
                tax = round(price * 0.07, 2)
                fee = calculate_facility_fee(price)
                total = round(price + tax + fee, 2)

                all_seats.append({
                    "venue_name": venue,
                    "event_name": event.get("event_name"),
                    "event_date": event.get("event_date"),
                    "event_time": event.get("event_time"),
                    "section": section,
                    "row": row,
                    "seat_no": seat,
                    "price": total,
                    "description": "",
                    "unique_id": event.get("performance_id", ""),
                    "timestamp": time.strftime("%d %b %Y %H:%M:%S", time.localtime())
                })

            except Exception as e:
                print(f"Error processing seat {sid}: {e}")
                continue

        print(f"Extracted {len(all_seats)} available seats")
        return all_seats

    except Exception as e:
        print(f"Error in get_seats: {e}")
        return []


def scrape_event( venue_name, event_unique_id, start_date, end_date, max_retries):
    """Main scraping function with optimizations"""
    global MAX_RETRIES, API_ENDPOINT_PREFIX, REQUEST_DELAY
    event_start_time = time.time()

    try:
        print(f"Starting scrape for event ID: {event_unique_id}")
        MAX_RETRIES = int(max_retries) if max_retries else 3
        API_ENDPOINT_PREFIX = venue_url.rstrip('/')
        REQUEST_DELAY = 0.3

        event_url = f"{API_ENDPOINT_PREFIX}/orderticketsvenue.asp?p={event_unique_id}"

        # Get events
        print("Fetching events...")
        events = get_events(url=venue_url, start_date=start_date, end_date=end_date,
                            timestamp_filter="", venue=venue_name)

        if not events:
            raise Exception("No events found")

        # Find target event
        target_event = next((e for e in events if str(e.get("performance_id")) == str(event_unique_id)), None)
        if not target_event:
            raise Exception(f"Event ID {event_unique_id} not found in {len(events)} events")

        print(f"Found target event: {target_event.get('event_name')}")

        # Check event status
        is_on_sale = target_event.get("event_on_sale", False)
        event_sale_status = target_event.get("event_sale_status", "")

        if not is_on_sale:
            raise Exception("Event not on sale")
        if any(kw in event_sale_status.lower() for kw in ["sold out", "cancelled", "postponed"]):
            raise Exception(f"Event status: {event_sale_status}")

        # Extract seats
        print("Extracting seat data...")
        seats_data = get_seats(event=target_event, venue=venue_name)

        if not seats_data:
            raise Exception("No seats found")

        # Log success
        duration = round((time.time() - event_start_time) / 60, 2)

        print(f"Successfully extracted {len(seats_data)} seats in {duration} minutes")

        return json.dumps({
            "status": "success",
            "message": f"Successfully extracted {len(seats_data)} seats",
            "event_data": seats_data
        })

    except Exception as e:
        duration = round((time.time() - event_start_time) / 60, 2)
        error_msg = f"Scraping failed: {str(e)}"
        print(error_msg)

        return json.dumps({
            "status": "error",
            "message": error_msg
        })


# Example usage
if __name__ == "__main__":
    output = scrape_event(
        "Gold Strike Casino Resort",
        "57",
        "2025-10-17",
        "2027-05-22",
        "3"
    )
    print(output)
