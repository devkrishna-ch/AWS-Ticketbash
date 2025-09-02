import pandas as pd
import re, time, json, logging
from datetime import datetime
from dateutil import parser
import requests
from bs4 import BeautifulSoup
import csv
import os
import html

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

VENUE_URL = "https://chanhassendt.com/wp-json/wpbm-audience-view/v1/shows"

def get_current_timestamp():
    return time.strftime("%d %b %Y %H:%M:%S", time.localtime())


def call_api_with_retries(method, url, headers=None, params=None, data=None, max_retries=3):
    delay = 2
    backoff_factor = 2
    for attempt in range(max_retries):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, data=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if response.status_code == 200:
                return response
            else:
                logger.warning(f"API call attempt {attempt+1} failed with status code {response.status_code} for url {url}. Retrying...")
                time.sleep(delay * (backoff_factor ** attempt))
        except Exception as e:
            logger.error(f"API call attempt {attempt+1} - exception occurred for url {url}. Retrying...: {str(e)}")
            time.sleep(delay * (backoff_factor ** attempt))
    
    raise Exception(f"Failed to get a successful response from {url} after {max_retries} retries.")


def get_all_events(url, venue, timestamp_filter, max_retries):
    events_list = []
    page_number = 1
    total_pages = 1
    try:
        if timestamp_filter == "":
            logger.info("No date time stamp filter provided")
            standardized_dates = []
        else:
            list_of_datesTime = timestamp_filter.split(",")
            standardized_dates = [
                datetime.strptime(date, "%Y-%m-%d %I:%M:%S %p").strftime("%Y-%m-%d %I:%M:%S %p")
                for date in list_of_datesTime
            ]

        while page_number <= total_pages:
            params = {"page": page_number, "limit": 100}
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json, text/plain, */*",
            }

            response = call_api_with_retries("GET", url, headers=headers, params=params, max_retries=max_retries)

            if response:
                logger.info(f"Request to fetch events for page number {page_number} is successful.")
                data = response.json()

                items = data.get('items', [])
                for item in items:
                    if not item.get("has_upcoming_performances", False):
                        continue

                    event_name = item.get("post_title", "")
                    ticket_url = item.get("ticket_link", "")
                    article_match = re.search(r'article_id=([A-F0-9-]+)', ticket_url, re.IGNORECASE)
                    article_id = article_match.group(1) if article_match else ""

                    for performance in item.get("upcoming_performances", []):
                        performance_id = performance.get("id", "")
                        event_date = performance.get("start_date", "").split(" ")[0]
                        event_time = performance.get("start_date", "").split(" ")[1]

                        try:
                            parsed_date = parser.parse(event_date + " " + event_time)
                            sdates = parsed_date.strftime("%Y-%m-%d %I:%M:%S %p")
                        except:
                            logger.error(f"Error parsing event date and time for event {event_name} at {ticket_url}")
                            sdates = ""

                        if standardized_dates:
                            if sdates in standardized_dates:
                                logger.info(f"{sdates} is present. Event {event_name}-{event_date} {event_time}")
                            else:
                                logger.warning(f"{sdates} is not present. Event {event_name}-{event_date} {event_time}")
                                continue

                        events_list.append({
                            "event_id": article_id,
                            "performance_id": performance_id,
                            "event_name": event_name,
                            "event_date": event_date,
                            "event_time": event_time,
                            "event_avl_code": performance.get("availability_status", ""),
                            "event_access": performance.get("access", ""),
                            "event_url": ticket_url
                        })

                total_pages = data.get("page_count", 1)
                if total_pages > page_number:
                    page_number += 1
                else:
                    break
            else:
                raise Exception("No response from events API.")

    except Exception as e:
        logger.error(f"An exception occurred while extracting events: {str(e)}")

    return events_list


def get_seats(event, venue, max_retries):
    seats_data = []
    price_info = {}
    sub_venue = ""
    try:
        performance_id = event.get("performance_id", "")
        url = "https://tickets.chanhassendt.com/Online/mapSelect.asp"

        headers = {"User-Agent": "Mozilla/5.0"}
        data = {"BOparam::WSmap::loadMap::performance_ids": performance_id, "createBO::WSmap": "1"}

        response = call_api_with_retries("POST", url, headers=headers, data=data, max_retries=max_retries)

        if response:
            logger.info("Request to fetch seat data successful.")
            soup = BeautifulSoup(response.text, 'html.parser')

            venue_element = soup.find("p", {"class": "performance-venue"})
            if venue_element:
                sub_venue = venue_element.text.split("\u2013")[0].split("-")[0].strip()

            script_tag = soup.find("script", string=re.compile("var currentSeats"))
            if script_tag:
                script_text = script_tag.string
                price_pattern = re.findall(r'totalsRendered\[\s*\'(.*?)\'\s*\]\s*\[\s*\'(.*?)\'\s*\]\s*=\s*[\'"](.*?)[\'"]', script_text)
                price_types_pattern = re.findall(r'priceTypes\["(.*?)"\]\s*=\s*"(.*?)"', script_text)

                price_type_dict = {seat_id: desc for seat_id, desc in price_types_pattern if desc.lower() != 'undefined'}
                for price_type_id, price_id, price_value in price_pattern:
                    price_value = float(price_value.replace("$", ""))
                    if any(kw in price_type_dict.get(price_type_id, "").lower().strip() for kw in ['concert only']):
                        price_info[price_id] = min(price_info.get(price_id, float("inf")), price_value)

            if not price_info:
                raise Exception("Price data not found.")

            for group in soup.select('.seatGroup > g'):
                seat_price_id = group.get('id', "")
                for circle in group.find_all('circle', {'data-status': 'A'}):
                    seat_desc = circle.get('data-tsmessage', '').strip()
                    if any(kw in seat_desc.lower() for kw in ['accessible', 'wheelchair', 'companion', 'obstructed']):
                        continue
                    price = price_info.get(seat_price_id, 0)
                    if price == 0:
                        continue
                    seats_data.append({
                        'Venue Name': venue,
                        'Event Name': event.get("event_name", ""),
                        'Event Date': event.get("event_date", ""),
                        'Event Time': event.get("event_time", ""),
                        'Section': circle.get('data-seat-section', '').strip(),
                        'Row': circle.get('data-seat-row', '').strip(),
                        'Seat': circle.get('data-seat-seat', '').strip(),
                        'Price': price,
                        'Desc': seat_desc,
                        'UniqueIdentifier': f"{performance_id}|{event.get('event_url','')}",
                        'TimeStamp': get_current_timestamp(),
                        'Sub Venue': sub_venue
                    })
        else:
            raise Exception("No response from seats API.")

    except Exception as e:
        logger.error(f"An exception occurred while extracting seat data: {str(e)}")

    return seats_data


def scrape_event(venue_name, event_unique_id, max_retry=3):
    """
    Scrapes a specific event using its unique ID (performance_id|event_url).
    
    Args:
        venue_url (str): Base URL of the venue.
        venue_name (str): Name of the venue.
        event_unique_id (str): Unique identifier in format 'performance_id|event_url'.
        max_retry (int): Max retries for API calls. Default = 3.

    Returns:
        str: JSON string with status, event_data, and message.
    """
    venue_url = VENUE_URL
    seats_data = []
    event_url = ""
    start_time = time.time()

    try:
        # --- Validate input ---
        if not event_unique_id:
            raise ValueError("Event unique id is not present.")
        if "|" not in event_unique_id:
            raise ValueError("Invalid format for event unique id. Expected 'performance_id|event_url'.")

        performance_id, event_url = event_unique_id.split("|", 1)
        if not performance_id.strip():
            raise ValueError("Performance id is missing in event unique id.")

        # --- Fetch all events ---
        events = get_all_events(
            url=venue_url, 
            venue=venue_name, 
            timestamp_filter="", 
            max_retries=max_retry
        )
        if not events:
            raise Exception("No events found at this venue.")

        # --- Match and scrape event ---
        for event in events:
            if str(event.get("performance_id")) == performance_id.strip():
                avl_status_code = str(event.get("avl_code", "")).lower().strip()

                if avl_status_code in {"s", "u"}:
                    raise Exception("Event sold out or unavailable.")

                # Get seat data
                seats_data = get_seats(
                    event=event, 
                    venue=venue_name, 
                    max_retries=max_retry
                )
                if not seats_data:
                    raise Exception("No seat data found for this event.")

                elapsed_time = round((time.time() - start_time) / 60, 2)
                success_msg = f"Scraping completed successfully in {elapsed_time} minutes"
                logger.info(f"[SUCCESS] {success_msg}")

                return json.dumps({
                    "status": "success",
                    "event_data": seats_data,
                    "message": success_msg
                })

        raise Exception("Event not found with given performance_id.")

    except Exception as e:
        elapsed_time = round((time.time() - start_time) / 60, 2)
        error_msg = f"An error occurred: {str(e)} | Time taken: {elapsed_time} minutes"
        logger.error(f"[ERROR] {error_msg}")

        return json.dumps({
            "status": "error",
            "event_data": seats_data,
            "message": error_msg
        })


# scrape_event("https://chanhassendt.com/wp-json/wpbm-audience-view/v1/shows","chanhassen dinner theatres","49116F41-E395-4580-B588-01DDA6FA772C|https://tickets.chanhassendt.com/Online/default.asp?doWork::WScontent::loadArticle=Load&BOparam::WScontent::loadArticle::article_id=FD1C8B53-A321-466C-B681-D53E928DA7FF&_gl=1*v53pxs*_gcl_aw*R0NMLjE3MjczNjE3NjcuQ2owS0NRandqTlMzQmhDaEFSSXNBT3hCTTZyNVN6MGpCM2VNQnBpQkRaUmdWTUg0NEdINWN6bm1MVm1XTmxveWRENWR0VlVSNUVJRTFGVWFBbHAyRUFMd193Y0I.*_gcl_au*NTI0Njk2MjE3LjE3MjczNjE3NjMuMTYyMDA4NjQ3Mi4xNzMzMzQzODI5LjE3MzMzNDM4Mjk.","chan/checker_output.xlsx","chan/logs.csv","3")