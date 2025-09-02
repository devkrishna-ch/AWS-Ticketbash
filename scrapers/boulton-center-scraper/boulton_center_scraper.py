import requests
import time
import json
import logging
from datetime import datetime
from dateutil import parser
from datetime import datetime
import random
from read_config import read_config

cfg = read_config()

proxy_auth = cfg["PROXY"]
proxies = {
    "http": "http://{}".format(proxy_auth),  # For SOCKS5, use socks5://
    "https": "http://{}".format(proxy_auth)  # Use the same for both HTTP and HTTPS
}

print(proxies)

client_id = "36822"

# PLATFORM_FEE = 1.075                                        #7.5% platform fee

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def retry_request(url, headers, max_retries=3, backoff_factor=2):
    """
    Make HTTP requests with exponential backoff retry logic for rate limiting.
    
    """
    logger.info(f"Making request to: {url}")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=30)
            
            if response.status_code == 200:
                logger.info(f"Request successful on attempt {attempt + 1}")
                return response
            elif response.status_code == 404:
                logger.error(f"Request failed with status code {response.status_code} on attempt {attempt + 1}")
                return response
            else:
                # retry
                # Rate limited - implement exponential backoff with jitter
                wait_time = backoff_factor ** attempt
                jitter = random.uniform(0, 1)
                total_wait = wait_time + jitter
                
                logger.warning(f"Too Many Requests. Retrying in {total_wait:.2f} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(total_wait)                
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception on attempt {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:
                raise Exception(f"Request failed after {max_retries} retries: {str(e)}")
            time.sleep(backoff_factor ** attempt)
    
    raise Exception(f"Request failed after {max_retries} retries.")

def get_events(client_id, timestamp_filter, venue):
    """
    Fetch and filter events from the Ovation Tix API.
    
    Args:
        client_id (str): Client identifier for the venue
        timestamp_filter (str): Comma-separated list of datetime filters (format: YYYY-MM-DD HH:MM:SS AM/PM)
        venue (str): Venue name for logging purposes
    
    Returns:
        list: List of event dictionaries containing event details
    """
    events_list = []
    logger.info(f"Fetching events for client_id: {client_id}, venue: {venue}")
    
    try:
        # Ovation Tix API endpoint for calendar productions
        url = "https://web.ovationtix.com/trs/api/rest/CalendarProductions"

        # Headers required for Ovation Tix API authentication and browser simulation
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
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "clientId": client_id,  # Critical: Client ID for venue identification
            "newCIRequest": "true",
            "Pragma": "no-cache",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
        }

        # Make API request with retry logic
        response = retry_request(url, headers)

        # Parse timestamp filter for event filtering
        timestamps_not_found = []
        if not timestamp_filter:
            logger.info("No datetime filter provided - will process all available events")
            standardized_dates = []
        else:
            logger.info(f"Processing timestamp filter: {timestamp_filter}")
            list_of_dates_time = timestamp_filter.split(",")
            
            # Standardize datetime format for comparison
            standardized_dates = []
            for date_str in list_of_dates_time:
                try:
                    parsed_dt = datetime.strptime(date_str.strip(), "%Y-%m-%d %I:%M:%S %p")
                    standardized_dates.append(parsed_dt.strftime("%Y-%m-%d %I:%M:%S %p"))
                except ValueError as e:
                    logger.error(f"Invalid datetime format '{date_str}': {str(e)}")
                    continue
            
            logger.info(f"Standardized {len(standardized_dates)} datetime filters")

        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched calendar data with {len(data)} date entries")
            
            # Process each date entry in the calendar
            for date_entry in data:   
                events = date_entry.get("productions", [])
                logger.debug(f"Processing {len(events)} events for date entry")
                
                # Process each event/production
                for event in events:
                    try:
                        # Extract basic event information
                        event_id = event.get("productionId", "")
                        event_name = event.get("name", "")
                        seat_selection_method = event.get("seatSelectionMethod", "")
                        shows = event.get("showtimes", [])
                        
                        logger.debug(f"Processing event: {event_name} (ID: {event_id}) with {len(shows)} showtimes")
                        
                        # Process each showtime for the event
                        for show in shows:
                            try:
                                # Parse showtime information
                                show_date_time = show.get("performanceStartTime", "")
                                if not show_date_time:
                                    logger.warning(f"No performance start time for show in event {event_name}")
                                    continue
                                
                                # Split date and time components
                                date_time_parts = show_date_time.split(" ")
                                if len(date_time_parts) < 2:
                                    logger.warning(f"Invalid datetime format: {show_date_time}")
                                    continue
                                    
                                event_date = date_time_parts[0]
                                event_time = " ".join(date_time_parts[1:])  # Handle AM/PM

                                # Parse and standardize datetime for filtering
                                try:
                                    parsed_date = parser.parse(f"{event_date} {event_time}")
                                    standardized_datetime = parsed_date.strftime("%Y-%m-%d %I:%M:%S %p")
                                except Exception as parse_error:
                                    logger.error(f"Failed to parse datetime '{show_date_time}': {str(parse_error)}")
                                    continue

                                # Apply timestamp filter if provided
                                if standardized_dates:
                                    if standardized_datetime in standardized_dates:
                                        logger.info(f"Event matches filter: {event_name} - {standardized_datetime}")
                                    else:
                                        timestamps_not_found.append(standardized_datetime)
                                        logger.debug(f"Event filtered out: {event_name} - {standardized_datetime}")
                                        continue

                                # Check event availability and status
                                is_available = show.get("performanceAvailable", False)
                                is_cancelled = show.get("isCancelled", False)
                                is_soldout = show.get("isSoldOut", False)
                                
                                # Skip unavailable, cancelled, or sold out events
                                if not is_available:
                                    logger.debug(f"Skipping unavailable event: {event_name}")
                                    continue
                                if is_cancelled:
                                    logger.debug(f"Skipping cancelled event: {event_name}")
                                    continue
                                if is_soldout:
                                    logger.debug(f"Skipping sold out event: {event_name}")
                                    continue
                                
                                # Add valid event to results
                                event_data = {
                                    "event_id": event_id,
                                    "event_name": event_name,
                                    "show_id": show.get("performanceId", ""),
                                    "event_date": event_date,
                                    "event_time": event_time,
                                    "seat_selection_method": seat_selection_method
                                }
                                events_list.append(event_data)
                                logger.debug(f"Added event: {event_name} - {standardized_datetime}")
                                
                            except Exception as show_error:
                                logger.error(f"Error processing show in event {event_name}: {str(show_error)}")
                                continue
                                
                    except Exception as event_error:
                        logger.error(f"Error processing event: {str(event_error)}")
                        continue
            
            logger.info(f"Successfully processed {len(events_list)} valid events")
            if timestamps_not_found:
                logger.info(f"Found {len(set(timestamps_not_found))} events that didn't match timestamp filter")
                
        else:
            error_msg = f"Request to fetch events failed with status code {response.status_code}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
    except Exception as e:
        error_msg = f"Exception occurred while fetching events: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    return events_list

def get_seats(client_id, performance_id, venue, event):
    """
    Fetch seat map and pricing data for a specific performance.
    
    Args:
        client_id (str): Client identifier for the venue
        performance_id (str): Unique identifier for the performance
        venue (str): Venue name
        event (dict): Event details containing name, date, time information
    
    Returns:
        list: List of seat dictionaries with pricing and location information
    """
    all_seats = []
    
    logger.info(f"Fetching seats for performance {performance_id} at {venue}")
    
    excluded_keywords = ["ada","accessible","sro","wheelchair","companion","obstructed","handicap"]
    included_keywords = ["theater","orchestra","advance","full","general","premium","preferred",
                         "reserved","standard","floor","balcony","stage","mezzanine","ticket","all ages"]

    try:
        url = f"https://web.ovationtix.com/trs/api/rest/Performance({performance_id})/seatingChart"
        params = {"deepLink": "true"}

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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "cache-control": "no-cache, no-store, must-revalidate",
            "clientId": client_id,
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 200:
            logger.info("Successfully fetched seating chart data")
            data = response.json()

            event_name = event.get("event_name", "")
            event_date = event.get("event_date", "")
            event_time = event.get("event_time", "")

            # Build price lookup dictionary from priceLevels
            price_levels = data.get("priceLevels", {})
            price_dict = {}

            logger.info(f"Processing {len(price_levels)} price levels")

            for price_level_id, price_level in price_levels.items():
                try:
                    price_level_name = price_level.get("name", "").strip().lower()
                    price_level_type = price_level.get("type", "").strip().lower()

                    # Skip excluded keywords in level name/type
                    if any(kw in price_level_type for kw in excluded_keywords):
                        continue
                    if any(kw in price_level_name for kw in excluded_keywords):
                        continue

                    for ticket in price_level.get("ticketTypes", []):
                        ticket_name = ticket.get("name", "").lower().strip()
                        ticket_price = ticket.get("priceIncludingFees", 0)

                        if "adult" in ticket_name and not any(kw in ticket_name for kw in excluded_keywords):
                            price_dict[int(price_level_id)] = {"price": float(ticket_price), "name": ticket_name}
                        elif "regular" in ticket_name and not any(kw in ticket_name for kw in excluded_keywords):
                            price_dict[int(price_level_id)] = {"price": float(ticket_price), "name": ticket_name}
                        elif any(kw in ticket_name for kw in included_keywords) and not any(kw in ticket_name for kw in excluded_keywords):
                            price_dict[int(price_level_id)] = {"price": float(ticket_price), "name": ticket_name}
                        elif "pwyw" in ticket_name:
                            if int(price_level_id) not in price_dict or ticket_price > price_dict[int(price_level_id)].get("price", 0):
                                price_dict[int(price_level_id)] = {"price": float(ticket_price), "name": ticket_name}

                        logger.debug(f"Price level {price_level_id}: ${ticket_price} ({ticket_name})")

                except Exception as e:
                    logger.warning(f"Invalid price data in price level {price_level_id}: {str(e)}")
                    continue

            if not price_dict:
                raise Exception("No valid price data found.")

            # Process sections and seats
            sections = data.get("sections", [])
            logger.info(f"Processing {len(sections)} sections")

            total_seats_processed = 0
            available_seats_found = 0

            for section in sections:
                try:
                    section_name = section.get("name", "Unknown Section")
                    rows = section.get("rows", [])

                    for row in rows:
                        try:
                            row_name = row.get("name", "")
                            seats = row.get("seats", [])

                            for seat in seats:
                                try:
                                    total_seats_processed += 1

                                    seat_number = seat.get("number", "")
                                    seat_row = seat.get("row", row_name)
                                    price_level_id = seat.get("priceLevel")
                                    is_available = seat.get("available", False)
                                    is_for_sale = seat.get("forSale", True)
                                    is_kill_seat = seat.get("killSeat", False)

                                    if not is_available or not is_for_sale or is_kill_seat:
                                        continue

                                    if str(seat_number).upper().startswith("WC"):
                                        continue

                                    seat_price = price_dict.get(price_level_id, {}).get("price", 0)
                                    if seat_price <= 0:
                                        continue
                                    # final_price = round(seat_price * PLATFORM_FEE,2)
                                    seat_record = {
                                        "Venue Name": venue,
                                        "Event Name": event_name,
                                        "Event Date": event_date,
                                        "Event Time": event_time,
                                        "Section": section_name,
                                        "Row": seat_row.strip().replace("Row:", "").strip(),
                                        "Seat": str(seat_number),
                                        "Price": seat_price,
                                        "Desc": "",
                                        "UniqueIdentifier": performance_id,
                                        "TimeStamp": time.strftime("%d %b %Y %H:%M:%S", time.localtime())
                                    }

                                    all_seats.append(seat_record)
                                    available_seats_found += 1

                                except Exception as seat_error:
                                    logger.error(f"Error processing individual seat: {str(seat_error)}")
                                    continue

                        except Exception as row_error:
                            logger.error(f"Error processing row {row_name} in section {section_name}: {str(row_error)}")
                            continue

                except Exception as section_error:
                    logger.error(f"Error processing section: {str(section_error)}")
                    continue

            logger.info(f"Processed {total_seats_processed} total seats, found {available_seats_found} available seats")

        else:
            error_msg = f"Request to fetch seats data failed with status code {response.status_code}"
            logger.error(f"{error_msg}. Response: {response.text[:200]}...")
            raise Exception(error_msg)

    except Exception as e:
        error_msg = f"Exception occurred while extracting seats: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    return all_seats

def get_event(client_id, performance_id):
    """
    Fetch detailed event information for a specific performance.
    
    """
    logger.info(f"Fetching event details for performance {performance_id}")
    
    try:
        # Ovation Tix performance details API endpoint
        url = f"https://web.ovationtix.com/trs/api/rest/Performance({performance_id})"

        # Standard headers for API requests
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
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "clientId": client_id,
            "newCIRequest": "true",
            "Pragma": "no-cache",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": '"Android"',
        }

        # Make API request
        response = retry_request(url, headers)
        
        if response.status_code == 200:
            logger.info("Successfully fetched event details")
            return response.json()
        else:
            error_msg = f"Request to fetch event failed with status code {response.status_code}"
            logger.error(error_msg)
            return None
            
    except Exception as e:
        error_msg = f"Exception occurred while extracting event data: {str(e)}"
        logger.error(error_msg)
        return None

def get_ga_seats(client_id, performance_id, venue, event):
    """
    Fetch general admission (GA) seat data for events with simple seat selection.

    """
    seat_data = []
    logger.info(f"Fetching GA seats for performance {performance_id} at {venue}")
    
    try:
        # Get detailed event information first
        event_data = get_event(client_id=client_id, performance_id=performance_id)
        if not event_data:
            error_msg = "Event data not found for GA seat processing"
            logger.error(error_msg)
            raise Exception(error_msg)

        logger.info("Processing GA seat data from event details")
        
        # Extract event details
        event_name = event.get("event_name", "")
        event_date = event.get("event_date", "")
        event_time = event.get("event_time", "")
        
        # Extract pricing information from event data
        price_including_fees = 0
        ga_max_tickets = 4  # Default maximum tickets for GA events
        max_tickets = 0
        
        # Process price codes to find GA pricing
        price_codes = event_data.get("priceCodes", [])
        logger.info(f"Processing {len(price_codes)} price codes for GA event")
        
        for price_code in price_codes:
            try:
                price_value = price_code.get("price", 0)
                max_qty = price_code.get("maxQuantity", 0)
                
                # Update maximum available tickets
                if max_qty > max_tickets:
                    max_tickets = max_qty
                
                # Use the highest price found (assuming premium GA pricing)
                if price_value > price_including_fees:
                    price_including_fees = price_value
                    
                logger.debug(f"GA price code: ${price_value}, max qty: {max_qty}")
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid GA price data: {str(e)}")
                continue
        
        # Use default max tickets if none specified
        if max_tickets == 0:
            max_tickets = ga_max_tickets
            logger.info(f"Using default max tickets: {max_tickets}")
        
        # Create GA seat records (one per available ticket)
        logger.info(f"Creating {max_tickets} GA seat records at ${price_including_fees} each")
        
        for i in range(1, max_tickets + 1):
            ga_seat = {
                "Venue Name": venue,
                "Event Name": event_name,
                "Event Date": event_date,
                "Event Time": event_time,
                "Section": "General Admission",
                "Row": "GA",
                "Seat": str(i),  # Sequential numbering for GA seats
                "Price": price_including_fees,
                "Desc": "General Admission",
                "UniqueIdentifier": performance_id,
                "TimeStamp": time.strftime("%d %b %Y %H:%M:%S", time.localtime())
            }
            seat_data.append(ga_seat)
            logger.debug(f"Added GA seat {i}")
        
        logger.info(f"Successfully created {len(seat_data)} GA seat records")
        
    except Exception as e:
        error_msg = f"Exception occurred while processing GA seats: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    return seat_data

def scrape_event(performance_id, venue_name):
    """
    Main scraping function to extract seat data for a specific event.

    Returns:
        str: JSON string containing scraping results with status, data, and messages
    """
    seats_data = []
    logger.info(f"Starting event scrape for performance {performance_id} at {venue_name}")
    
    try:
        # Extract client ID from venue URL
        logger.info(f"Extracted client ID: {client_id}")
        
        if not client_id:
            error_msg = "Client ID not present in venue URL"
            logger.error(f"{error_msg}. Performance ID: {performance_id}")
            raise Exception(error_msg)
        
        # Get specific event data directly
        logger.info(f"Fetching event details for performance {performance_id}")
        event_data = get_event(client_id=client_id, performance_id=performance_id)
        
        if not event_data:
            error_msg = f"Event with performance ID {performance_id} not found"
            logger.error(error_msg)
            return json.dumps({
                "status": "error",
                "event_data": [],
                "message": error_msg
            })
        
        # Extract event details from API response
        production = event_data.get("production", {})
        event_name = production.get("productionName", "")
        
        # Parse start date/time
        performance_start_time = event_data.get("startDate", "")
        if " " in performance_start_time:
            event_date = performance_start_time.split(" ")[0]
            event_time_raw = performance_start_time.split(" ")[1]
            
            # Ensure time is in HH:MM:SS format
            try:
                time_parts = event_time_raw.split(":")
                if len(time_parts) == 2:
                    event_time = f"{event_time_raw}:00"
                else:
                    event_time = event_time_raw
            except:
                event_time = "00:00:00"
        else:
            event_date = performance_start_time
            event_time = "00:00:00"
        
        # Create event object for seat functions
        target_event = {
            "event_name": event_name,
            "event_date": event_date,
            "event_time": event_time,
            "show_id": performance_id
        }
        
        # Determine seat selection method and scrape accordingly
        seat_selection_mode = production.get("seatSelectionMethod", "").upper()
        logger.info(f"Event seat selection method: {seat_selection_mode}")
        
        if seat_selection_mode in ["USER", "BOTH"]:
            logger.info("Processing reserved seating event")
            seats_data = get_seats(
                client_id=client_id,
                performance_id=performance_id,
                venue=venue_name,
                event=target_event
            )
        elif seat_selection_mode == "SYSTEM":
            logger.info("Processing general admission event")
            seats_data = get_ga_seats(
                client_id=client_id,
                performance_id=performance_id,
                venue=venue_name,
                event=target_event
            )
        else:
            error_msg = f"Unknown seat selection method: {seat_selection_mode}"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        if not seats_data:
            error_msg = "No seats found"
            logger.warning(error_msg)
            raise Exception(error_msg)
        
        # Return successful result
        success_message = f"Successfully scraped {len(seats_data)} seats"
        logger.info(success_message)
        
        return json.dumps({
            "status": "success",
            "event_data": seats_data,
            "message": success_message
        })
        
    except Exception as e:
        error_msg = f"Exception occurred while scraping event: {str(e)}"
        logger.error(error_msg)
        
        return json.dumps({
            "status": "error",
            "event_data": seats_data,
            "message": error_msg
        })


# performance_id = "11631960"
# venue_name = "Boulton Center for the Performing Arts"
# output = scrape_event(performance_id, venue_name)

# print(json.dumps(json.loads(output)["event_data"], indent=2))