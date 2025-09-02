import os
import random
import time
import json
import re
import html
from curl_cffi import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
import bs4


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from read_config import read_config
config = read_config()
proxy_auth = config.get('PROXY')
# Configure proxy settings for both HTTP and HTTPS requests
proxies = {
    "http": f"http://{proxy_auth}",
    "https": f"http://{proxy_auth}"
}

logger.info(f"Proxy auth: {proxy_auth}")

def get_seatmap_ids(url, event_id):
    seatmap_ids = []

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    }
    endpoint = f"{url}/?itemNumber={event_id}#/seatmap"
    print(endpoint)
    response = requests.get(url=endpoint, headers=headers, impersonate="chrome120", proxies=proxies, timeout=60,
                            verify=False)

    if response.status_code == 200:
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        script_tags = soup.find_all("script")
        for script_tag in script_tags:
            if "window.appSettings" in script_tag.text:
                start_index = script_tag.text.find("window.appSettings = '{") + len(
                    "window.appSettings = '"
                )
                end_index = script_tag.text.find("';", start_index)
                app_settings_text = script_tag.text[start_index:end_index]
                cleaned_text = app_settings_text.replace(r"\"", '"')
                pattern = r'"seatmap":\{"id":"(.*?)"'
                seatmap_ids = re.findall(pattern, cleaned_text)

                break
        if not seatmap_ids:
            return response.status_code, None
        else:
            return response.status_code, seatmap_ids
    else:
        response.status_code, response.content

def get_event_seatmap(url, seatmap_id, event_id, event_name):
    headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        # 'cookie': cookie_string,
        "origin": "https://www.kennedy-center.org",
        "content-type": "application/json",
        "priority": "u=1, i",
        "referer": "https://www.kennedy-center.org/checkout/smartseat/",
        "sec-ch-ua": '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    }

    json_data = {
        "itemType": 0,
        "itemId": event_id,
        "allowSeparatedSeats": False,
        "selectedAllLevels": True,
        "allowAisleAccessSelected": False,
        "seatMapId": seatmap_id,
        "language": None,
        "seatsToIgnore": [],
        "isSyosOnly": False,
    }

    logging.info(f"Making seatmap API call for event_id: {event_id}, seatmap_id: {seatmap_id}")
    logging.info(f"API URL: {url}")
    logging.info(f"Request payload: {json_data}")

    try:
        response = requests.post(url, headers=headers, json=json_data, impersonate="chrome120", proxies=proxies, timeout=60,
                                 verify=False)
        logging.info(f"Response status: {response.status_code}")

        if response.status_code == 200:
            response_json = response.json()
            logging.info(f"Response received successfully, data keys: {list(response_json.keys()) if response_json else 'No data'}")
            return response.status_code, response_json
        else:
            logging.warning(f"API call failed with status {response.status_code}")
            logging.warning(f"Response text: {response.text[:500]}...")  # Log first 500 chars
            return response.status_code, response.reason

    except Exception as e:
        logging.error(f"Exception during API call: {e}")
        return 500, str(e)

def get_event_tickets(data, venue_details):
    available_seat = next(
        (
            seatstyle
            for seatstyle in data.get("seatStyles")
            if seatstyle.get("name") == "Available"
        ),
        None,
    )

    if available_seat:
        available_seat_id = available_seat["id"] if available_seat else None

        all_seat_pricing = data.get("allSeatPricing")
        zone_prices = {
            str(zone["zoneId"]): zone.get("prices", [{}])[0].get("price", 0)
                                 + zone.get("prices", [{}])[0].get("feeAmount", 0)
            for zone in all_seat_pricing
        }

        scraped_tickets = []

        for seats in data.get("levelSeats", []):
            tessitura_seat = seats.get("tessituraSeat")
            if tessitura_seat:
                seat_details = tessitura_seat
                if seat_details.get("isAvailable") is True and (
                        seats.get("seatStyleId") == available_seat_id
                ):
                    section_name = seat_details.get("sectionDescription", "")
                    section_id = seat_details.get("sectionId")
                    seat_number = seat_details.get("numberText")
                    row = seat_details.get("rowText", "")
                    zone_id = seat_details.get("zoneId")
                    zone_description = seats.get("zoneDescription", "")
                    seat_status = seat_details.get("isAvailable")
                    seat_type = "regular"
                    scraped_tickets.append(
                        {
                            "Venue Name": venue_details.get("venueName"),
                            "Event Name": venue_details.get("eventName"),
                            "Event Date": venue_details.get("eventDate"),
                            "Event Time": venue_details.get("eventTime"),
                            # "Ticket Type": "Standard",
                            "Section": section_name,
                            "Row": row,
                            "Seat": seat_number,
                            "Price": zone_prices.get(zone_id),
                            "Desc": zone_description,
                            # "Venue ID": venue_details.get("venueID"),
                            # "Zone Description": zone_description,
                            "UniqueIdentifier": venue_details.get("eventId"),
                            "TimeStamp": venue_details.get("timeStamp"),
                            "Seat Type": seat_type,
                        }
                    )
        return scraped_tickets
    else:
        logging.warning("Couldn't find any standard available seats.")

def extract_event_data(url, seatmap_id, event_id, event_name, venue_details, max_retries=3):
    retries = 0

    while retries < max_retries:
        try:
            response = get_event_seatmap(url, seatmap_id, event_id, event_name)
            status_code = response[0]

            if status_code == 200:
                response_data = response[1]
                extracted_seats = get_event_tickets(response_data, venue_details)
                df = pd.DataFrame(extracted_seats)
                if retries>0:
                    logging.info(f"Seatmap data fetched after {retries} retries.")
                return df  # Exit the retry loop upon success
            elif status_code == 429:
                delay = random.randint(5, 8)
                logging.warning(
                    f"Received 429 Too Many Requests. Retrying after {delay} seconds..."
                )
                time.sleep(delay)
                retries += 1

            elif status_code == 500:
                delay = random.randint(3, 5)
                logging.warning(f"Received 500 Response. Retrying after {delay} seconds...")
                time.sleep(delay)
                retries += 1

            else:
                logging.warning(
                    f"Failed to get seatmap. Status code: {status_code}. Error: {response[1]}"
                )
                delay = random.randint(3, 5)
                time.sleep(delay)
                retries += 1
        except Exception as e:
            logging.warning(f"{e} Retrying...")
            retries += 1
    else:
        logging.error(
            f"Failed to retrieve seatmap after {max_retries} retries"
        )
        return pd.DataFrame()  # Return empty DataFrame instead of None

def get_seatmap_ids_from_event_id(seatmap_id_url, event_id, venue_name,event_name="", max_retries=4):
    retries = 0
    if venue_name == "Kennedy Center Theater Lab":
        return ["8e3990a2-f3ac-475f-9f75-adcc96ef2c23"]
    elif venue_name == "Kennedy Center Eisenhower Theater":
        return ["88c037f8-85d7-4b6f-ba75-c60daa4d152d","b9ae6b48-4f04-4126-a696-dbb51b040282","def92836-2a9e-4511-a465-eabef562a0c7"]
    elif venue_name == "Kennedy Center Terrace Theater":
        return ["66b5111e-2ec6-4a79-8144-6a6f44ad43a5"]
    elif venue_name == "Kennedy Center Family Theater":
        return ["76edbf0c-0afb-4626-80c9-987d5b7e8ae4"]
    elif venue_name == "Kennedy Center Concert Hall":
        return ["beb3f28f-285f-4048-bcdb-8b7eff417e45","6e31f4d0-134d-429d-ac3d-a08717148f68","13caa55d-19b8-450c-a5b5-af0a939c099e","08a3040b-f95e-4252-9fc6-a54277e7a20a"]
    elif venue_name == "Kennedy Center Opera House":
        return ["768cf3c3-8d4a-499f-ab4b-b5a79d1fb552","be589d78-31de-4744-af6b-ce116f4c86e1","514c8046-0615-4811-8658-6bc967970a7f","3ad65181-a52f-47f6-b6ef-a27c71d1b00d"]
    while retries <= max_retries:
        try:
            sm_response = get_seatmap_ids(seatmap_id_url, event_id)
            status_code = sm_response[0]
            if status_code == 200:
                seatmap_ids = sm_response[1]
                if seatmap_ids is None:
                    seatmap_ids = [""]
                if retries > 0:
                    logging.info(f"Seatmaps retrieved after {retries} retries")
                break

            else:
                delay = random.randint(5, 10)
                logging.warning(
                    f"Received {status_code} while getting Seatmap IDs for {event_name}_{event_id} Retrying after {delay} seconds..."
                )
                time.sleep(delay)
                retries += 1
        except Exception as e:
            delay = random.randint(3, 5)
            logging.error(
                f"Error occured while getting Seatmap IDs for {event_name}_{event_id}: {e}. Retrying after {delay} seconds.."
            )
            time.sleep(delay)
            retries += 1
    else:

        logging.warning(
            f"Failed to retrieve seatmap ids for {event_name}_{event_id} after {max_retries}. Attempting extraction with no seatmapID now."
        )
        seatmap_ids = [""]
    return seatmap_ids

def parse_event_identifier(event_identifier):
    """
    Parse event identifier in format: venue_name|event_name|event_date|event_time|event_id
    Example: Kennedy Center Family Theater|Little Murmur|2025-10-25|11:00:00|86350
    Returns: (venue_name, event_name, event_date, event_time, event_id)
    """
    try:
        parts = event_identifier.split('|')
        if len(parts) != 5:
            raise ValueError(f"Event identifier must have 5 parts separated by '|', got {len(parts)}")

        venue_name = parts[0].strip()
        event_name = parts[1].strip()
        event_date = parts[2].strip()  # Format: YYYY-MM-DD
        event_time = parts[3].strip()  # Format: HH:MM:SS
        event_id = parts[4].strip()

        # Validate date format
        datetime.strptime(event_date, "%Y-%m-%d")
        # Validate time format
        datetime.strptime(event_time, "%H:%M:%S")

        return venue_name, event_name, event_date, event_time, event_id
    except Exception as e:
        raise ValueError(f"Invalid event identifier format: {e}")

def create_venue_details_from_identifier(venue_name, event_name, event_date, event_time, event_id, seatmap_url):
    """
    Create venue details dictionary from parsed event identifier
    """
    venue_details = {
        "seatmap_url": seatmap_url,
        "timeStamp": datetime.now().strftime("%d %b %Y %H:%M:%S"),
        "venueName": venue_name,
        "venueID": "",  # Not available from identifier
        "eventName": event_name,
        "eventDate": event_date,
        "eventTime": event_time,
        "ticketURL": "",  # Not available from identifier
        "eventId": event_id,
    }
    return venue_details

def check_event(seatmap_url, seatmap_id_url, event_identifier, max_retries=3):
    success_string, error_string = "success", "error"
    start_time = time.perf_counter()

    logger.info("Checker started for The Kennedy Center.")

    if not event_identifier:
        logger.warning("Event identifier not passed. Ending process")
        return json.dumps({"status": error_string, "event_data": [], "message": "event_identifier not provided"})

    try:
        venue_name, event_name, event_date, event_time, event_id = parse_event_identifier(event_identifier)
        logger.info(f"Parsed identifier - Venue: {venue_name}, Event: {event_name}, Date: {event_date}, Time: {event_time}, ID: {event_id}")
    except ValueError as e:
        logger.error(f"Invalid event identifier format: {e}")
        return json.dumps({"status": error_string, "event_data": [], "message": f"Invalid identifier: {e}"})

    try:
        venue_details = create_venue_details_from_identifier(
            venue_name, event_name, event_date, event_time, event_id, seatmap_url
        )
        logger.info("Created venue details")

        seatmap_ids = get_seatmap_ids_from_event_id(seatmap_id_url, event_id, venue_name, event_name, max_retries)
        logger.info(f"Fetched seatmap_ids: {seatmap_ids}")

        event_df_list = []
        for seatmap_id in seatmap_ids:
            logger.info(f"Extracting event data for seatmap_id={seatmap_id}")
            event_df = extract_event_data(seatmap_url, seatmap_id, event_id, event_name, venue_details, max_retries=3)
            if event_df is not None and not event_df.empty:
                event_df_list.append(event_df)

        if not event_df_list:
            raise Exception("No event data extracted")

        final_df = pd.concat(event_df_list, ignore_index=True)
        return json.dumps({"status": success_string, "event_data": final_df.to_dict(orient="records"), "message": ""})

    except Exception as e:
        logger.error(f"Error processing event {event_identifier}: {e}")
        return json.dumps({"status": error_string, "event_data": [], "message": f"Error: {e}"})
    finally:
        elapsed_time = time.perf_counter() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        logger.info(f"Checker process complete for {event_identifier}")
        logger.info(f"Elapsed time: {int(hours)}h {int(minutes)}m {seconds:.2f}s")
        logger.info("=" * 70)


