import json
import requests
import time
import logging
import re

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

# Client ID for the Hunterdon Hills Playhouse venue on OvationTix platform
client_id = "36253"
# Maximum number of retry attempts for failed API calls
MAX_RETRIES = 3

# Configure logging to track scraper operations and debug issues
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safe_json(response):
    """Safely parse a requests.Response as JSON with helpful diagnostics.

    Raises ValueError if the Content-Type is not JSON or the body is invalid JSON.
    """
    ctype = (response.headers.get("Content-Type") or "").lower()
    body = response.text
    if "application/json" not in ctype:
        raise ValueError(
            f"Non-JSON response ({response.status_code}, {ctype}): {body[:200]!r}"
        )
    try:
        return response.json()
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}; body[:200]={body[:200]!r}")

def safe_str(value):
    """Return a safe string for JSON serialization without manual escaping.

    Always rely on json.dumps for escaping; this just guarantees a string type.
    """
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)

def call_api_with_retries(method, url, headers=None, params=None, data=None):
    """
    Calls an API with automatic retries using exponential backoff.
    """
    delay = 5  # Initial delay in seconds before first retry
    backoff_factor = 2  # Multiplier for exponential backoff (5s, 10s, 20s...)

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
                # Skip unsupported HTTP methods
                print(f"[call_api_with_retries] Unsupported method: {method}")
                continue

            print(f"[call_api_with_retries] Status Code: {response.status_code}")

            # Return immediately on successful response
            if response.status_code == 200:
                return response
            # Don't retry on 404 (resource not found) - it won't change
            elif response.status_code == 404:
                print(f"[call_api_with_retries] Status {response.status_code}, returning None.")
                return None
            else:
                # Retry for other status codes (5xx server errors, rate limits, etc.)
                print(f"[call_api_with_retries] Retryable status code: {response.status_code}")
                # Wait with exponential backoff before next attempt
                time.sleep(delay * (backoff_factor ** attempt))

        except Exception as e:
            # Handle network errors, timeouts, and other exceptions
            print(f"[call_api_with_retries] Exception occurred: {e}")
            # Wait before retrying after an exception
            time.sleep(delay * (backoff_factor ** attempt))

    # All retry attempts exhausted
    print("[call_api_with_retries] All retries failed, returning None.")
    return None

def get_seats(client_id, performance_id, venue,event):
    all_seats = []
    price_dict = {}
    excluded_keywords = ["accessible","sro","wheelchair","companion","obstructed","handicap"]
    included_keywords = ["premium","preferred","reserved","standard","floor","balcony","stage","mezzanine"]
    try:
        url = f"https://web.ovationtix.com/trs/api/rest/Performance({performance_id})/seatingChart"

        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://ci.ovationtix.com",
            "Referer": "https://ci.ovationtix.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Mobile Safari/537.36",
            "clientId": client_id,
            "newCIRequest": "true",
        }


        response = call_api_with_retries("GET", url, headers=headers)

        if response.status_code == 200:
            print("request to fetch seat data successfull.")
            data = safe_json(response)

            sections = data.get("sections",[])
            prices = data.get("priceLevels",{})
            event_name = event.get("event_name")
            event_date = event.get("event_date")
            event_time = event.get("event_time")


            for price_id, price_obj in prices.items():
                price_level_name = price_obj.get("name","").strip().lower()

                price_level_type = price_obj.get("type","").strip().lower()

                if any(kw in price_level_type for kw in excluded_keywords):
                    continue

                if any(kw in price_level_name for kw in excluded_keywords):
                    continue

                for ticket in price_obj.get("ticketTypes",[]):
                    ticket_name = ticket.get("name","").lower().strip()
                    ticket_price = ticket.get("priceIncludingFees",0)
                    if "adult" in ticket_name and not any(kw in ticket_name for kw in excluded_keywords):
                        price_dict[price_id] = {"price" :ticket_price, "name" :ticket_name}
                    elif "regular" in ticket_name and not any(kw in ticket_name for kw in excluded_keywords):
                        price_dict[price_id] = {"price" :ticket_price, "name" :ticket_name}
                    elif any(kw in ticket_name for kw in included_keywords) and not any(kw in ticket_name for kw in excluded_keywords):
                        price_dict[price_id] = {"price" :ticket_price, "name" :ticket_name}
                    elif "pwyw" in ticket_name:
                        if price_id not in price_dict or ticket_price > price_dict[price_id].get("price",0):
                            price_dict[price_id] = {"price": ticket_price, "name": ticket_name}



            if not price_dict:
                raise Exception("No price data found.")

            for section in sections:
                rows = section.get("rows",[])
                for row in rows:
                    seats = row.get("seats",[])
                    for seat in seats:
                        is_available = seat.get("available")
                        for_sale = seat.get("forSale")
                        price_id = str(seat.get("priceLevel"))

                        if (not is_available) or (not for_sale):
                            continue

                        # if any(kw in price_dict[price_id]['name'].lower() for kw in ["wheelchair","ada","companion","accessible"]):
                        #     continue

                        seat_price = price_dict.get(price_id,{}).get("price",0)

                        if seat_price <= 0:
                            continue

                        section_name = seat.get("sectionName","").strip()
                        row_name = seat.get("row","").strip()
                        seat_number_str = seat.get("number","").strip()
                        if "-" in seat_number_str and not row_name:
                            row_name = seat_number_str.split("-")[0]
                            seat_number = seat_number_str.split("-")[1]
                        else:
                            seat_number = seat_number_str

                        all_seats.append({
                            "Venue Name": safe_str(venue),
                            "Event Name": safe_str(event_name),
                            "Event Date": safe_str(event_date),
                            "Event Time": safe_str(event_time),
                            "Section": safe_str(section_name),
                            "Row": safe_str(row_name),
                            "Seat": safe_str(seat_number),
                            "Price": safe_str(seat_price),
                            "Desc":safe_str(""),
                            "UniqueIdentifier": safe_str(performance_id),
                            "TimeStamp": time.strftime("%d %b %Y %H:%M:%S", time.localtime())
                        })
            # with open("SanPedro/formatted_seats.json","w") as f:
            #     json.dump(all_seats,f,indent=4)
        else:
            print(f"Request to fetch seats data failed with status code {response.status_code}")
            current_time = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
            print(current_time, performance_id, venue, event.get("event_name"), f"{event.get('event_date', '')} {event.get('event_time', '')}", "NA", "NA", "Request to fetch seats data failed with status code: " + str(response.status_code))
    except Exception as e:
        print("An exception has occurred while extracting seats.")
        current_time = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
        print(current_time, performance_id, venue, event.get("event_name"), f"{event.get('event_date', '')} {event.get('event_time', '')}", "NA", "NA", "An error occurred while extracting seats. " + str(e))

    return all_seats

def get_ga_seats(client_id, performance_id,venue, event):
    seat_data = []
    price_including_fees = 0
    max_tickets = 0
    section_required_keywords = ["standard", "general", "adult", "general admission","gen","main","classroom","univest"]
    ticket_name_required_keywords = ["standard", "general", "general admission","floor","balcony","stage","saturday eve or sunday","saturday eve/sunday","saturday eve", "sunday eve","sunday performance","saturday performance"]
    excluded_keywords = ["vip", "reserved", "box", "premium","accessible","wheelchair"]
    highest_pwyw_ticket = None
    try:
        event_data = get_event(client_id=client_id, performance_id=performance_id, venue=venue)
        if not event_data:
            raise Exception("Event Data not found")
        
        tickets_available = event_data.get("ticketsAvailable")
        available_to_purchase_on_web = event_data.get("availableToPurchaseOnWeb")

        if tickets_available and available_to_purchase_on_web:
            for section in event_data.get("sections", []):
                section_name = section["ticketGroupName"].lower().strip()
                
                if any(kw in section_name for kw in section_required_keywords) and not any(kw in section_name for kw in excluded_keywords):
                        # Find the highest priced PWYW ticket in a single line
                    highest_pwyw_ticket = max(
                        (ticket for ticket in section["ticketTypeViews"] if ("pwyw" in ticket["name"].lower().strip() or "pay what you will" in ticket["name"].lower().strip())),
                        key=lambda t: t["priceIncludingFees"],
                        default=None
                    )
                    for ticket in section["ticketTypeViews"]:
                        ticket_name = ticket["name"].lower().strip()

                        if "adult" in ticket_name and not any(kw in ticket_name for kw in excluded_keywords):
                            price_including_fees = ticket["priceIncludingFees"]
                            max_tickets = ticket["maxTickets"]
                            break
                        elif any(kw in ticket_name for kw in ticket_name_required_keywords) and not any(kw in ticket_name for kw in excluded_keywords):
                            price_including_fees = ticket["priceIncludingFees"]
                            max_tickets = ticket["maxTickets"]
                            break
                        elif "student" in ticket_name and not any(kw in ticket_name for kw in excluded_keywords):
                            price_including_fees = ticket["priceIncludingFees"]
                            max_tickets = ticket["maxTickets"]
                            break

                        elif ("pwyw" in ticket_name or "pay what you will" in ticket_name)and highest_pwyw_ticket:
                            price_including_fees = highest_pwyw_ticket["priceIncludingFees"]
                            max_tickets = highest_pwyw_ticket["maxTickets"]


                if max_tickets > 0:
                    break

        if price_including_fees > 0:
            seat_data.append({
                    "Venue Name": safe_str(venue),
                    "Event Name": safe_str(event.get("event_name")),
                    "Event Date": safe_str(event.get("event_date")),
                    "Event Time": safe_str(event.get("event_time")),
                    "Section": safe_str("General Admission"),
                    "Row": safe_str("GA"),
                    "Seat": safe_str(""),
                    "Price": safe_str(price_including_fees),
                    "Desc":safe_str(f"{max_tickets}-max seat"),
                    "UniqueIdentifier": performance_id,
                    "TimeStamp": time.strftime("%d %b %Y %H:%M:%S", time.localtime())
            })
    except Exception as e:
        print("An exception has occurred while extracting ga seats.")
        current_time = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
        print(current_time, performance_id, venue, event.get("event_name"), f"{event.get('event_date', '')} {event.get('event_time', '')}", "NA", "NA", "An error occurred while extracting ga seats. " + str(e))

    return seat_data

def get_event(client_id, performance_id, venue):
    data = None
    try:
        url = f"https://web.ovationtix.com/trs/api/rest/Performance({performance_id})"

        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://ci.ovationtix.com",
            "Referer": "https://ci.ovationtix.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Mobile Safari/537.36",
            "clientId": client_id,
            "newCIRequest": "true",
        }

        response = call_api_with_retries("GET", url, headers=headers)
        if response.status_code == 200:
            print("Request to fetch event is successfull.")
            data = safe_json(response)
        else:
            print(f"Request to fetch event failed with status code {response.status_code}")
            current_time = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
            print(current_time, "NA", venue, performance_id, "NA", "NA", "NA", "Request to fetch event failed with status code: " + str(response.status_code))
    except Exception as e:
        print("An exception has occurred while extracting event data.")
        current_time = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
        print(current_time, "NA", venue, performance_id, "NA", "NA", "NA", "Error occurred while fetching event. " + str(e))
    return data

def scrape_event(venue_url, performance_id, venue_name):
    """
    Main scraping function to extract seat data for a specific event.

    This is the primary entry point for scraping seat data. It determines the event type
    (reserved seating vs general admission) and calls the appropriate scraping function.

    Args:
        venue_url (str): URL of the venue (used for client ID extraction)
        performance_id (str): Unique identifier for the specific performance
        venue_name (str): Name of the venue

    Returns:
        str: JSON string containing scraping results with status, data, and messages
    """
    seats_data = []  # Store scraped seat data
    logger.info(f"Starting event scrape for performance {performance_id} at {venue_name}")

    try:
        # Log the client ID being used (hardcoded for Walhalla venue)
        logger.info(f"Extracted client ID: {client_id}")

        # Validate client ID is available
        if not client_id:
            error_msg = "Client ID not present in venue URL"
            logger.error(f"{error_msg}. URL: {venue_url}")
            raise Exception(error_msg)

        # Fetch event details to determine scraping approach
        logger.info(f"Fetching event details for performance {performance_id}")
        event_data = get_event(client_id=client_id, performance_id=performance_id, venue=venue_name)

        # Handle case where event is not found
        if not event_data:
            error_msg = f"Event with performance ID {performance_id} not found"
            logger.error(error_msg)
            return json.dumps({
                "status": "error",
                "event_data": [],
                "message": error_msg
            })

        # Extract event metadata from API response
        production = event_data.get("production", {})
        event_name = safe_str(production.get("productionName", ""))

        # Parse and format start date/time
        performance_start_time = event_data.get("startDate", "")
        if " " in performance_start_time:
            # Split date and time components
            event_date = performance_start_time.split(" ")[0]
            event_time_raw = performance_start_time.split(" ")[1]

            # Ensure time is in HH:MM:SS format for consistency
            try:
                time_parts = event_time_raw.split(":")
                if len(time_parts) == 2:
                    event_time = f"{event_time_raw}:00"  # Add seconds if missing
                else:
                    event_time = event_time_raw
            except:
                event_time = "00:00:00"  # Default time if parsing fails
        else:
            # Handle date-only format
            event_date = performance_start_time
            event_time = "00:00:00"

        # Create standardized event object for seat extraction functions
        target_event = {
            "event_name": event_name,
            "event_date": event_date,
            "event_time": event_time,
            "show_id": performance_id
        }

        # Determine seating type and call appropriate scraping function
        seat_selection_mode = production.get("seatSelectionMethod", "")
        if not seat_selection_mode:
            seat_selection_mode = "SYSTEM"

        seat_selection_mode = seat_selection_mode.upper()

        logger.info(f"Event seat selection method: {seat_selection_mode}")

        # Handle reserved seating events (user selects specific seats)
        if seat_selection_mode in ["USER", "BOTH", "U", "B"]:
            logger.info("Processing reserved seating event")
            seats_data = get_seats(
                client_id=client_id,
                performance_id=performance_id,
                venue=venue_name,
                event=target_event
            )
        # Handle general admission events (system assigns seats)
        elif seat_selection_mode in ["SYSTEM", "S"]:
            logger.info("Processing general admission event")
            seats_data = get_ga_seats(
                client_id=client_id,
                performance_id=performance_id,
                venue=venue_name,
                event=target_event
            )
        else:
            # Handle unknown seating methods
            error_msg = f"Unknown seat selection method: {seat_selection_mode}"
            logger.error(error_msg)
            raise Exception(error_msg)

        # Validate that seats were found
        if not seats_data:
            error_msg = "No seats found"
            logger.warning(error_msg)
            raise Exception(error_msg)

        # Return successful scraping result
        success_message = f"Successfully scraped {len(seats_data)} seats"
        logger.info(success_message)

        return json.dumps({
            "status": "success",
            "event_data": seats_data,
            "message": success_message
        })

    except Exception as e:
        # Handle any unexpected errors during scraping
        error_msg = f"Exception occurred while scraping event: {str(e)}"
        logger.error(error_msg)

        # Return error result with any partial data collected
        return json.dumps({
            "status": "error",
            "event_data": seats_data,
            "message": error_msg
        })

# Example usage and testing section
if __name__ == "__main__":
    """
    Test script to demonstrate scraper functionality.

    This section runs when the script is executed directly (not imported).
    It provides a working example of how to use the scraper with sample data.
    """
    # Sample test data for Hunterdon Hills Playhouse
    test_venue_url = "https://ci.ovationtix.com/36253"  # Hunterdon Hills Playhouse venue URL
    test_performance_id = "11579690"  # Sample performance ID
    test_venue_name = "Hunterdon Hills Playhouse"  # Venue display name

    logger.info("Starting test scrape")
    # Execute the main scraping function
    result = scrape_event(test_venue_url, test_performance_id, test_venue_name)

    # Parse and display the results for testing/debugging
    try:
        result_data = json.loads(result)
        print(f"Status: {result_data.get('status')}")
        print(f"Message: {result_data.get('message')}")

        event_data = result_data.get('event_data', [])
        print(f"Seats found: {len(event_data)}")

        # Display complete seat data if available
        if event_data:
            print("\n--- COMPLETE SEAT DATA ---")
            print(json.dumps(event_data, indent=2))

            # Also show a sample of the first few seats for quick review
            print(f"\n--- SAMPLE SEATS (first 5) ---")
            for i, seat in enumerate(event_data[:5]):
                print(f"Seat {i+1}: {json.dumps(seat, indent=2)}")

    except json.JSONDecodeError:
        # Handle case where result is not valid JSON
        print(f"Raw result: {result}")

