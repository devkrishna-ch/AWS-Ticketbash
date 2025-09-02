import requests
import re
import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from dateutil.parser import parse
from datetime import datetime
import logging


venue_url = "https://app.spektrix-link.com/clients/athenstheatre/eventsView.json"


# Global EVENT_MAPPING is updated via update_event_mapping().
EVENT_MAPPING = {}

# Hard-coded area mapping (areaId -> section name)
AREA_MAP = {
    "201": "Athens Theatre",
    "202": "Main Left",
    "203": "Main Center",
    "204": "Main Right",
    "205": "Balcony Left",
    "206": "Balcony Center",
    "207": "Balcony Right",
    "208": "Boxes"
}

logger = logging.getLogger(__name__)

def get_current_timestamp():
    return time.strftime("%d %b %Y %H:%M:%S", time.localtime())
##############################################
# MAPPING API FUNCTIONS (NEW)
##############################################

def get_mapping_api(venue):
    """
    Calls the mapping.json API and returns its JSON as a dictionary.
    """
    url = "https://app.spektrix-link.com/clients/athenstheatre/mapping.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            mapping = response.json()
            print("Mapping API returned:")
            # print(mapping)
            return mapping
        else:
            print(f"Failed to retrieve mapping API data. Status code: {response.status_code}")
            raise Exception(f"Failed to retrieve mapping API data. Status code: {response.status_code}")
    except Exception as e:
        print("Exception occurred while fetching mapping API:", e)
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | Exception occurred while fetching mapping API: {e}")
        return {}

def get_events_view_mapping(venue, url):
    """
    Calls the eventsView.json API, and for each event:
      - Uses "webEventId" if available (or the event name with spaces removed) as the key.
      - Extracts the first 5 numeric characters from "lastAvailableInstanceId" (if present)
        as the value; otherwise, duplicates the key.
    Returns a mapping dictionary.
    """
    # url = "https://app.spektrix-link.com/clients/athenstheatre/eventsView.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    mapping = {}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            events = response.json()
            for event in events:
                key = event.get("webEventId") or event.get("name", "").replace(" ", "")
                last_avail = event.get("lastAvailableInstanceId", "")
                if last_avail:
                    digits = re.findall(r'\d', str(last_avail))
                    if digits:
                        value = "".join(digits[:5])
                    else:
                        value = key
                else:
                    value = key
                mapping[key] = value
            print("EventsView API mapping constructed:")
            # print(mapping)
        else:
            print(f"Failed to retrieve eventsView API data. Status code: {response.status_code}")
            raise Exception(f"Failed to retrieve eventsView API data. Status code: {response.status_code}")
    except Exception as e:
        print("Exception occurred while fetching eventsView API:", e)
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | Exception occurred while fetching eventsView API: {e}")
    return mapping

def update_event_mapping(venue,url):
    """
    Updates the global EVENT_MAPPING using the new mapping API functions.
    Combines mapping.json and eventsView.json results, filtering for entries where
    the event code is numeric (i.e. contains only digits).
    """
    try:
        global EVENT_MAPPING
        mapping_api = get_mapping_api(venue=venue)
        events_view_mapping = get_events_view_mapping(venue=venue,url=url)
        mapping_api_pairs = list(mapping_api.items())
        events_view_pairs = list(events_view_mapping.items())
        combined_pairs = mapping_api_pairs + events_view_pairs
        numeric_pairs = [(k, v) for k, v in combined_pairs if v.isdigit()]
        EVENT_MAPPING = dict(numeric_pairs)
        print("\nFinal EVENT_MAPPING (only numeric entries):")
        # for k, v in EVENT_MAPPING.items():
        #     print(f'"{k}": "{v}",')
    except Exception as e:
        print(f"An exception has occurred while updating event mapping: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | An exception has occurred while updating event mapping: {e}")


##############################################
# HELPER FUNCTIONS
##############################################

def split_datetime(dt_str):
    """
    Attempt to parse dt_str with dateutil.parser and return (yyyy-mm-dd, HH:MM:SS).
    If parsing fails, return (dt_str, '').
    """
    try:
        dt = parse(dt_str)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
    except Exception as e:
        print(f"Failed to parse datetime '{dt_str}': {e}")
        return dt_str, ""

def extract_price(text):
    price = 0
    try:
        match = re.search(r"\$\d+\.\d+", text)

        if match:
            price = match.group().replace("$", "")  # Extracts "$42.60"
            print(price)

    except:
        price = 0
    return price


import tempfile
import uuid
def get_seating_html(instance_id, venue):
    """
    Use the given numeric instance_id to build the seating URL,
    and use Selenium to retrieve the fully rendered HTML.
    """
    seating_url = f"https://tickets.athensdeland.com/athenstheatre/website/ChooseSeats.aspx?EventInstanceId={instance_id}&culture=en-US&resize=true"
    html = ""
    driver = None

    try:
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--blink-settings=imagesEnabled=false")

        # ✅ Fix: Create a unique user data dir every time
        unique_dir = tempfile.mkdtemp(prefix=f"chrome-user-data-{uuid.uuid4()}")
        options.add_argument(f"--user-data-dir={unique_dir}")

        driver = webdriver.Chrome(options=options)
        driver.get(seating_url)

        time.sleep(20)  # Optional: Replace with WebDriverWait

        html = driver.page_source

    except Exception as e:
        print(f"An exception has occurred while getting seat html: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | An exception has occurred while getting seat html: {e}")
    finally:
        if driver:
            driver.quit()
    return seating_url, html, instance_id

def extract_event_info(html, venue):
    """
    Use BeautifulSoup to extract event name, date/time, and venue name.
    """
    event_name, event_datetime, venue_name = "","",""
    try:
        soup = BeautifulSoup(html, "html.parser")
        event_name_elem = soup.find("span", class_="EventName")
        event_name = event_name_elem.get_text(strip=True) if event_name_elem else "Unknown Event"
        
        dt_elem = soup.find("span", class_="DateAndTime")
        event_datetime = dt_elem.get_text(strip=True) if dt_elem else "2025-10-18T17:00:00"
        
        venue_elem = soup.find("span", class_="VenueName")
        venue_name = venue_elem.get_text(strip=True) if venue_elem else "Athens Theatre"
    except Exception as e:
        print("An exception has occurred while extracting event info:"+str(e))
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | An exception has occurred while extracting event info: {str(e)}")
        
    return event_name, event_datetime, venue_name

##############################################
# EVENT & INSTANCE DATA FUNCTIONS
##############################################

def get_event_json(event_code, venue):
    """
    Build the API URL for a given event code and return its JSON data.
    """
    base_url = "https://app.spektrix-link.com/clients/athenstheatre/events/"
    url = f"{base_url}{event_code}.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: Received status code {response.status_code} for event code {event_code}")
            raise Exception(f"Error: Received status code {response.status_code} for event code {event_code}")
    except Exception as e:
        print(f"Exception while fetching event code {event_code}: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | Exception while fetching event code {event_code}: {e}")
        return None

def process_event_json(data, venue):
    """
    Extract event-level data from the JSON.
    """
    event_record = {}
    try:
        event_record = {
            "Event Name": data.get("name", ""),
            "Description": data.get("description", ""),
            "HTML Description": data.get("htmlDescription", ""),
            "Duration": data.get("duration", ""),
            "Image URL": data.get("imageUrl", ""),
            "Thumbnail URL": data.get("thumbnailUrl", ""),
            "Instance Dates": data.get("instanceDates", ""),
            "Web Event ID": data.get("webEventId", ""),
            "Event ID": data.get("id", ""),
            "First Instance DateTime": data.get("firstInstanceDateTime", ""),
            "Last Instance DateTime": data.get("lastInstanceDateTime", ""),
            "Attribute Genre": data.get("attribute_Genre", ""),
            "Attribute Coursestorm": data.get("attribute_Coursestorm", "")
        }
    except Exception as e:
        print(f"An exception has occurred while processing event json: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | An exception has occurred while processing event json: {e}")
    return event_record

def process_instances(data, venue):
    """
    Extract instance-level data from the event JSON.
    """
    instance_records = []
    try:
        event_name = data.get("name", "")
        first_instance = data.get("firstInstanceDateTime", "")
        last_instance = data.get("lastInstanceDateTime", "")
        instances = data.get("instances", [])
        for instance in instances:
            try:
                availability = instance.get("availability", {})
                record = {
                    "Event Name": event_name,
                    "First Instance": first_instance,
                    "Last Instance": last_instance,
                    "Instance Start": instance.get("start", ""),
                    "Instance Start UTC": instance.get("startUtc", ""),
                    "Start Selling At": instance.get("startSellingAtWeb", ""),
                    "Stop Selling At": instance.get("stopSellingAtWeb", ""),
                    "Available Seats": availability.get("available", ""),
                    "Capacity": availability.get("capacity", ""),
                    "Unavailable Seats": availability.get("unavailable", ""),
                    "Plan ID": instance.get("planId", ""),
                    "Price List ID": instance.get("priceList", {}).get("id", ""),
                    "Instance ID": instance.get("id", ""),
                    "Cancelled": instance.get("cancelled", False)
                }
                instance_records.append(record)
            except:
                pass
    except Exception as e:
        print(f"An exception has occurred while processing instances: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | An exception has occurred while processing instances: {e}")

    return instance_records

def get_general_admission_price(html: str, venue: str) -> float:
    """
    • First try the legacy PriceListTable/green‑cell scrape.  
    • If that fails, look for the *first* “$xx.xx … (GA|general|admission)” pattern
      within 150 chars – covers new Spektrix templates.  
    Returns 0.0 if nothing is found.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")

        # ── 1️⃣ legacy table scrape ──────────────────────────────────────────
        table = soup.find("table", class_="PriceListTable")
        if table:
            for td in table.select("tr td.TicketType"):
                txt = td.get_text(" ", strip=True)
                m   = re.search(r"\$(\d+(?:\.\d+)?)", txt)
                if m:
                    return float(m.group(1))

        # ── 2️⃣ generic GA price regex fallback ──────────────────────────────
        #   …\$xx.xx… (within ~150 chars) …general|admission|ga…
        patt = re.compile(
            r"\$(\d{1,3}(?:\.\d{2})?)"
            r"(.{0,150}?)"
            r"(?:general\s*admission|general\s*ticket|ga\b)",
            re.I | re.S,
        )
        m = patt.search(html)
        if m:
            return float(m.group(1))

    except Exception as exc:
        print(f"[GA‑price] {venue}: scrape error: {exc}")

    return 0.0


def scrape_seat_data(html, venue):
    """
    Use a regex to find the seatData string in the page source.
    Then split on ";" for each seat record and on "|" for fields.
    """
    parsed_records = []
    try:
        match = re.search(r'seatData["\']?\s*:\s*["\']([^"\']+)["\']', html, re.DOTALL)
        if not match:
            print("seatData not found in the page source.")
            return None
        seat_data_str = match.group(1)
        print("Extracted seatData string:")
        # print(seat_data_str)
        records = seat_data_str.split(";")
        parsed_records = []
        for rec in records:
            rec = rec.strip()
            if rec:
                fields = rec.split("|")
                parsed_records.append(fields)
    except Exception as e:
        print(f"An exception has occurred while scraping seat data: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | NA | NA | NA | NA | An exception has occurred while scraping seat data: {e}")
    return parsed_records

def map_seat_records(raw_records, venue, event_name, event_date, event_time, seating_url, general_price, numeric_id):
    """
    Extract seat prices directly from the seat data instead of using color mapping.
    """
    final_rows = []
    
    try:
        timestamp = get_current_timestamp()
        ev_date, _ = split_datetime(event_date)  # Only use date part
        
        # Convert event_time from "7:30PM" to "19:30:00" format
        if event_time and ("AM" in event_time or "PM" in event_time):
            try:
                # Strip whitespace first, then parse "7:30PM" format
                clean_time = event_time.strip()
                time_obj = datetime.strptime(clean_time, "%I:%M%p")
                ev_time = time_obj.strftime("%H:%M:%S")
            except ValueError:
                try:
                    # Try with space: "7:30 PM"
                    clean_time = event_time.strip()
                    time_obj = datetime.strptime(clean_time, "%I:%M %p")
                    ev_time = time_obj.strftime("%H:%M:%S")
                except ValueError:
                    ev_time = event_time.strip()
        else:
            ev_time = event_time.strip() if event_time else "00:00:00"
        
        for raw_record in raw_records:
            fields = raw_record.split("|") if isinstance(raw_record, str) else raw_record
            
            if len(fields) < 12:
                continue
                
            color_code = fields[4].strip() if len(fields) > 4 else ""
            
            # Skip unavailable seats (gray)
            if color_code == "cccccc":
                continue
                
            # Extract seat info from field 11 (contains "L10 - $35.00")
            seat_info = fields[11].strip() if len(fields) > 11 else ""
            
            # Extract price directly from seat info
            price_match = re.search(r'\$(\d+(?:\.\d+)?)', seat_info)
            seat_price = float(price_match.group(1)) if price_match else general_price
            
            # Extract row and seat from seat info
            seat_match = re.search(r'^([A-Za-z]+)(\d+)', seat_info)
            if seat_match:
                row_val = seat_match.group(1)
                seat_val = seat_match.group(2)
            else:
                continue
                
            area_id = fields[1].strip() if len(fields) > 1 else ""
            section_name = AREA_MAP.get(area_id, area_id)
            
            record = {
                "Venue Name": venue,
                "Event Name": event_name,
                "Event Date": ev_date,
                "Event Time": ev_time,  # ✅ Now uses converted time
                "Section": section_name,
                "Row": row_val,
                "Seat": seat_val,
                "Price": seat_price,  # ✅ Price extracted from seat data
                "Desc": "",
                "UniqueIdentifier": f"{numeric_id}|{event_name}|{ev_date}|{ev_time}",  # ✅ Correct time
                "TimeStamp": timestamp
            }
            final_rows.append(record)
            
    except Exception as e:
        print(f"An exception has occurred while mapping seat records: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} | {event_name} | {event_date} {event_time} | NA | NA | An exception has occurred while mapping seat records: {e}")
    return final_rows

def get_seats(event_json, venue):
    """
    Given an event JSON and the venue name, process its instances,
    retrieve seating data for each instance (using Selenium to get the full HTML),
    scrape the general admission price, extract seatData, map seat records,
    and return a list of seat record dictionaries.
    """
    seats = []
    try:
        event_record = process_event_json(event_json, venue=venue)
        instances = process_instances(event_json, venue=venue)
        for instance in instances:
            instance_id = instance.get("Instance ID", "")
            if instance_id:
                seating_url, html, numeric_id = get_seating_html(instance_id, venue=venue)
                if html is None:
                    continue
                general_price = get_general_admission_price(html, venue=venue)
                raw_records = scrape_seat_data(html, venue=venue)
                if not raw_records:
                    continue
                mapped = map_seat_records(raw_records, venue,
                                        event_record.get("Event Name", ""),
                                        event_record.get("First Instance DateTime", ""),
                                        "",  # time can be extracted from the datetime if available
                                        seating_url, general_price, numeric_id=numeric_id)
                seats.extend(mapped)
    except Exception as e:
        print(f"An exception has occurred while extracting seats: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} | NA | {venue} |  |  | NA | NA | An exception has occurred while extracting seats: {e}")
    return seats


def get_ga_seats(show_id: str, venue: str, event_obj: dict) -> list[dict]:
    """
    Fallback: scrape GA price & qty for a single EventInstanceId.
    """
    from datetime import datetime, timezone   # ✅ ensure timezone is in scope
    url  = f"https://tickets.athensdeland.com/athenstheatre/website/ChooseSeats.aspx?EventInstanceId={show_id}&culture=en-US&resize=true"
    hdrs = {"User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"}

    try:
        html = requests.get(url, headers=hdrs, timeout=15).text
    except Exception as exc:
        logger.warning("GA page fetch failed for %s: %s", show_id, exc)
        return []

    price_m = re.search(r"\$(\d+(?:\.\d+)?)", html)
    qty_m   = re.search(r"(?:max(?:imum)?|available)\s*(\d+)", html, re.I)

    price = float(price_m.group(1)) if price_m else 0.0
    qty   = int(qty_m.group(1)) if qty_m else None         # qty is optional

    if price == 0:
        logger.info("GA scrape found no price for %s", show_id)
        return []

    return [{
        "Venue Name": venue,
        "Event Name": event_obj["event_name"],
        "Event Date": event_obj["event_date"],
        "Event Time": event_obj["event_time"],
        "Section":    "General Admission",
        "Row":        "GA",
        "Seat":       "",
        "Price":      price,
        "Desc":       f"{qty}-max seat" if qty else "",
        "UniqueIdentifier": f"{show_id}|{event_obj['event_name']}|{event_obj['event_date']}|{event_obj['event_time']}",
        "TimeStamp":  datetime.now(timezone.utc).strftime("%d %b %Y %H:%M:%S"),
    }]


##############################################
# SCRAPE VENUE & SCRAPE EVENT FUNCTIONS
##############################################

def scrape_event(venue_name, event_unique_id):
    """
    Process a single event given an event_unique_id in the format:
    "performance_id|Event Name|Event Date|Event Time".
    Retrieve seating data for that performance, write Excel output,
    and log processing details.
    """
    try:
        update_event_mapping(venue=venue_name, url=venue_url)
        
        parts = event_unique_id.split("|")
        if len(parts) < 4:
            raise Exception("Invalid event unique id format.")
        performance_id, event_name, event_date, event_time = parts[0], parts[1], parts[2], parts[3]
        
        seating_url = f"https://tickets.athensdeland.com/athenstheatre/website/ChooseSeats.aspx?EventInstanceId={performance_id}&culture=en-US&resize=true"
        # Load the seating page via Selenium
        html = None
        try:
            seating_url, html, numeric_id = get_seating_html(performance_id, venue=venue_name)
        except Exception as e:
            raise Exception(f"Error loading seating page: {e}")
        
        if not html:
            raise Exception("No seating html found.")
        
        general_price = get_general_admission_price(html, venue=venue_name)
        
        raw_records = scrape_seat_data(html, venue=venue_name)
        if raw_records:        
            seats_data = map_seat_records(raw_records, venue_name, event_name, event_date, event_time, seating_url, general_price, numeric_id=numeric_id)
        else:
            logger.info("No seat data found. Trying GA fallback.")
            seats_data = get_ga_seats(performance_id, venue_name, {"event_name": event_name, "event_date": event_date, "event_time": event_time})
            
        if not seats_data:
            raise Exception("No seats processed")

        
        current_time = get_current_timestamp()
        print(f"{current_time} | {seating_url} | {venue_name} | {event_name} | {event_date} {event_time} |  |  | ")

        

        return json.dumps({
            "status": "success",
            "message": "",
            "event_count": str(len(seats_data)),
            "event_data": seats_data  # 👈 add this!
        })

    except Exception as e:
        print(f"An exception has occurred while scraping the event: {e}")
        current_time = get_current_timestamp()
        print(f"{current_time} |  | {venue_name} | {event_unique_id} |  |  |  | An exception has occurred while scraping the event: {e}")
        return json.dumps({
            "status": "error", 
            "message": f"An exception has occurred while scraping the event: {e}", 
            "event_count": "0",
            "data": []
        })

##############################################
# MAIN
##############################################

# if __name__ == "__main__":


#     venue_url = "https://app.spektrix-link.com/clients/athenstheatre/eventsView.json"  # Example URL (not used directly in this logic)
#     venue_name = "Athens Theatre"
#     event_unique_id = "36601|2026 The ROCKET MAN Show - An Elton John Tribute | Friday, January 2, 2026 | 19:30:00"
#     # event_unique_id= "26602|2025 Rocky Horror Picture Show|2025-10-31|23:00:00"
#     event_result = scrape_event(venue_name, event_unique_id)
#     print("scrape_event result:", event_result)








