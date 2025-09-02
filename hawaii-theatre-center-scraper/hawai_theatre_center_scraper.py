# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium import webdriver
import json
import requests
import time
import logging
import re
from datetime import datetime

# Custom module to read configuration settings
from read_config import read_config

# Load configuration settings from external config file
config = read_config()
# Extract proxy authentication details from config
proxy_auth = config.get('PROXY')
# Configure proxy settings for both HTTP and HTTPS requests
proxies = {
    "http": f"http://{proxy_auth}",
    "https": f"http://{proxy_auth}"
}

print(f"Proxy auth: {proxy_auth}")

VENUE_URL = "https://hawaiitheatre.my.salesforce-sites.com/ticket/#"
PLATFORM_FEE = 0
MAX_RETRIES = 3
DICT_SEATING = """{"orchestracenter":"Orchestra Center","balconyleftcenter":"Balcony Left Center","balconyleft":"Balcony Left","balconyright":"Balcony Right","balconyrightcenter":"Balcony Right Center","upperbalconycenter":"Upper Balcony Center","upperbalconyleftcent":"Upper Balcony Left Center","upperbalconyleft":"Upper Balcony Left","upperbalconyrightcen":"Upper Balcony Right Center","upperbalconyright":"Upper Balcony Right","logeleftcenter":"Loge Left Center","logeleft":"Loge Left","logeright":"Loge Right","logerightcenter":"loge Right Center","orchestraleft":"Orchestra Left","orchestraright":"Orchestra Right","parterreleft":"Parterre Left","parterreright":"Parterre Right","logecenter":"Loge Center"}"""
ARIA_LABEL = "TUMBLER + 1 BEVERAGE Quantity,Charcuterie Quantity"

# Configure logging to track scraper operations and debug issues
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

def check_alert(driver):
    try:
        # Locate and wait the alert div
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "alert-danger")))
        alert = driver.find_element(By.CLASS_NAME, "alert-danger")
        
        # Extract text from the alert
        alert_text = alert.text
        print("Alert Message:", alert_text)

        #close the alert message
        close_button = driver.find_element(By.CLASS_NAME, "close")
        close_button.click() 
        
        # Use regex to find the maximum quantity in the message
        match = re.search(r"maximum of (\d+) per order", alert_text)
        
        if match:
            max_quantity = int(match.group(1))
            print("Maximum Quantity Allowed:", max_quantity)
            return max_quantity
        else:
            print("No quantity restriction found.")
            return None  
        
    except Exception as e:
        print("No alert found.")
        return None

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
                        "seating_type": seating_type,
                        "event_url": event_url
                    })
                except Exception as e:
                    logger.error(f"[{venue}] Error parsing event: {e}")

    except Exception as e:
        logger.error(f"[{venue}] Error fetching events: {e}")

    return events_data, tokens

def get_ga_seats(event, venue):
    # global ARIA_LABEL
    data = []
    no_of_tickets = 6
    item_total = 0
    per_ticket_price = 0
    # ARIA_LABEL = ARIA_LABEL.split(",")

    # Set up Chrome options
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--ignore-certificate-errors')

    driver = None
    try:
        # Attempt to initialize the Chrome driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get(event["event_url"])

        try:
            add_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[" 
                    "starts-with(translate(normalize-space(@aria-label), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'add') "
                    "and contains(translate(@aria-label, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'tickets') "
                    "and contains(@class, 'btn-primary')]"
                ))
            )
            print("Found 'Add Standard tickets' button, clicking it...")
            add_button.click()
            print("Clicked 'Add Standard tickets' button.")
            time.sleep(2)
        except:
            print("No 'Add Standard tickets' button found, continuing...")

        # Wait for the event page to load and the quantity input field to appear
        input_field = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((
                By.XPATH,
                " | ".join([
                    f"//input[substring(@aria-label, string-length(@aria-label) - string-length('Quantity') + 1) = 'Quantity']"
                ])
            ))
        )
        print("Input field for quantity found.")
        input_field.clear()  # Clears any existing value
        print("Cleared input field.")
        input_field.send_keys(str(no_of_tickets))  # Enter the number of tickets
        print(f"Entered {no_of_tickets} tickets in the input field.")

        try:
            add_to_cart_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[starts-with(normalize-space(text()), 'Add') "
                    "and substring(normalize-space(text()), string-length(normalize-space(text())) - string-length('Cart') + 1) = 'Cart' "
                    "and contains(@class, 'btn-primary')]"
                ))
            )
            print("Found 'Add Cart' button, clicking it...")
            add_to_cart_button.click()
            print("Clicked 'Add to Cart' button successfully.")
            time.sleep(2)
        except:
            print("No matching 'Add Cart' button found")
        
        # Check for any alerts after attempting to add to cart
        max_allowedQnt=check_alert(driver)
        print("Maximum allowed quantity:", max_allowedQnt)
        if max_allowedQnt:
            no_of_tickets = max_allowedQnt
            print(f"Maximum allowed quantity is {no_of_tickets}. Updating input field.")

            # Clear the input field and enter the new quantity
            input_field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    " | ".join([
                        f"//input[substring(@aria-label, string-length(@aria-label) - string-length('Quantity') + 1) = 'Quantity']"
                    ])
                ))
            )
            
            input_field.clear()
            input_field.send_keys(str(no_of_tickets))
            add_to_cart_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[starts-with(normalize-space(text()), 'Add') "
                    "and substring(normalize-space(text()), string-length(normalize-space(text())) - string-length('Cart') + 1) = 'Cart' "
                    "and contains(@class, 'btn-primary')]"
                ))
            )
            print("Found 'Add to Cart' button")
            add_to_cart_button.click()
            print("Clicked 'Add to Cart' button successfully.")
            time.sleep(2)

        # Wait for the item total to be visible
        item_total_span = WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".text-right.col span.total"))
        )
        print("Item total span found.")
        print(item_total_span.text.split("$"))
        item_total = float(item_total_span.text.split("$")[1])
        print(f"Item total extracted: ${item_total}")
        per_ticket_price = str(item_total / no_of_tickets)
        print(f"Per ticket price calculated: ${per_ticket_price}")

        # Create the data for the tickets
        current_time = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
        data = [{
            'Venue Name': venue,
            'Event Name': event["event_name"],
            'Event Date': event["event_date"],
            'Event Time': event["event_time"],
            'Section': "General Admission",
            'Row': "GA",
            'Seat': "",
            'Price': per_ticket_price,
            'Desc': str(no_of_tickets)+"-max seats",
            'UniqueIdentifier': event["event_url"],
            'TimeStamp': current_time
        }]

    except Exception as e:
        current_time = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
        logger.exception(f"[{venue}] Error occurred while extracting GA seats for {event.get('event_name')} on {event.get('event_date')} {event.get('event_time')}: {e}")
        print("An error occurred while extracting GA seats:", e)
    finally:
        if driver:
            driver.quit()
    return data

def get_seats(event, tokens, venue):
    global DICT_SEATING
    eventUrl = event.get("event_url", "")
    seats_data = []

    # Convert string data into dictionary
    if not DICT_SEATING:
        DICT_SEATING = {}
    elif isinstance(DICT_SEATING, str):
        try:
            DICT_SEATING = json.loads(DICT_SEATING)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid dictionary string format: {e}")
            return seats_data

    # Extract the domain from the URL and ensure it includes https
    domain_match = re.search(r"(https?://[A-Za-z_0-9.-]+)", eventUrl)
    domain = domain_match.group(1) if domain_match else "unknown"
    if not domain.startswith("https://"):
        domain = "https://" + domain
    logger.info(f"[{venue}] Using domain: {domain}")

    try:
        url = f"{domain}/ticket/apexremote"
        event_id = event["event_id"]

        # Headers
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": f"{domain}",
            "Referer": f"{domain}/ticket",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "X-User-Agent": "Visualforce-Remoting",
        }

        # Payload
        data = {
            "action": "PatronTicket.Controller_PublicTicketApp",
            "method": "fetchEventDescriptor",
            "data": [event_id, "", ""],
            "type": "rpc",
            "tid": 6,
            "ctx": {
                "csrf": tokens.get("fetchEventDescriptor", {}).get("csrf", ""),
                "vid": tokens.get("vid", ""),
                "ns": tokens.get("fetchEventDescriptor", {}).get("ns", ""),
                "ver": tokens.get("fetchEventDescriptor", {}).get("ver", ""),
                "authorization": tokens.get("fetchEventDescriptor", {}).get("authorization", "")
            },
        }

        # Call API with retries
        response = call_api_with_retries("POST", url, headers=headers, data=data)
        if not response:
            logger.error(f"[{venue}] API call failed after retries for event {event.get('event_name')}")
            return seats_data

        response_data = response.json()
        status_code = response_data[0].get("statusCode", 0)
        if status_code != 200:
            logger.error(f"[{venue}] Request to fetch seats failed with status code: {status_code}")
            return seats_data

        result = response_data[0].get("result")
        if not result.get("active"):
            logger.info(f"[{venue}] Event inactive or no seat data for {event.get('event_name')}")
            return seats_data

        price_groups = result.get("allocList", [])
        seats = result.get("seatList", [])
        subvenue = result.get("venue", {}).get("name", "")

        price_group_data = {
            pg["id"]: {
                "price_desc": pg["name"],
                "max_price": pg["maxPrice"],
                "min_price": pg["minPrice"]
            }
            for pg in price_groups
        }

        for seat in seats:
            try:
                if seat["avail"] and not any(
                    kw in str(seat.get("note", "")).lower()
                    for kw in ['companion', 'wheelchair', 'accessible', 'obstructed']
                ):
                    seat_name = seat.get("snName", "").lower().strip()
                    if any(kw in seat_name for kw in ['companion', 'wheelchair', 'accessible', 'obstructed']):
                        continue

                    seat_info_str = seat.get("key", "")
                    section_str = seat_info_str.split(":")[0].strip().lower()
                    row = seat_info_str.split(":")[1].replace("~", " ") if ":" in seat_info_str else ""

                    section_name = DICT_SEATING.get(section_str, section_str) if DICT_SEATING else section_str
                    if not section_name:
                        continue

                    price_id = seat.get("taId", "")
                    price = float(price_group_data.get(price_id, {}).get("max_price", 0))
                    if price <= 0:
                        continue

                    price += float(PLATFORM_FEE)
                    seat_no = ''.join([char for char in seat_info_str.split(":")[2] if char.isdigit()]) if ":" in seat_info_str else ""
                    desc = seat.get("note", "")

                    seats_data.append({
                        "Venue Name": venue,
                        "Event Name": event.get("event_name", ""),
                        "Event Date": event.get("event_date", ""),
                        "Event Time": event.get("event_time", ""),
                        "Section": section_name,
                        "Row": row,
                        "Seat": seat_no,
                        "Price": price,
                        "Desc": desc,
                        "UniqueIdentifier": event.get("event_url", ""),
                        "TimeStamp": time.strftime("%d %b %Y %H:%M:%S", time.localtime()),
                        # "Sub Venue": subvenue
                    })
            except Exception as e:
                logger.warning(f"[{venue}] Exception while adding seat: {e}")

    except Exception as e:
        logger.exception(f"[{venue}] Error occurred while extracting seats for {event.get('event_name')}: {e}")

    return seats_data

def scrape_event(event_url, venue_name):
    seats_data = []
    try:
        event_start_time = time.time()

        # Extract event ID from the URL using regex
        match = re.search(r"instances/([^,]+)", event_url)
        if not match:
            raise Exception("Failed to extract event ID from the URL.")
        event_id = match.group(1)
        logger.info(f"[{venue_name}] Extracted Event ID: {event_id}")

        # Fetch all events
        events, tokens = get_events(venue_name)
        if not events:
            raise Exception("No events found.")

        # Search for the specific event
        target_events = [event for event in events if event["event_id"] == event_id]
        if not target_events:
            raise Exception(f"Event ID {event_id} not found in event list.")

        for event in target_events:
            # if event["is_soldout"]:
            #     raise Exception("Event is sold out.")

            # Fetch seat information
            if event.get("seating_type", "").lower().strip() == "general admission":
                # seats_data = get_ga_seats(event, venue_name)
                continue
            else:
                seats_data = get_seats(event, tokens, venue_name)

            # Validate that seats were found
            if not seats_data:
                raise Exception("No seats found for the event.")

            # Successful scrape
            success_message = f"Successfully scraped {len(seats_data)} seats"
            event_end_time = time.time()
            total_event_time = round((event_end_time - event_start_time) / 60, 2)
            logger.info(f"[{venue_name}] {success_message}. Took {total_event_time} minutes.")

            return json.dumps({
                "status": "success",
                "event_data": seats_data,
                "message": success_message
            })

    except Exception as e:
        event_end_time = time.time()
        total_event_time = round((event_end_time - event_start_time) / 60, 2)
        error_msg = f"Exception occurred while scraping event {event_url}: {str(e)}. Took {total_event_time} minutes."
        logger.error(f"[{venue_name}] {error_msg}")

        return json.dumps({
            "status": "error",
            "event_data": seats_data,
            "message": error_msg
        })
    
# scrape_event("https://hawaiitheatre.my.salesforce-sites.com/ticket/#/instances/a0FUl000005smKvMAI","hawaii theater center")