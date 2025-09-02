import random
import time
import re
from curl_cffi import requests
import pandas as pd
import logging
from datetime import datetime, timedelta


def call_events_list_api(url, start_date, end_date):
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.6",
        "origin": "https://www.kennedy-center.org",
        "referer": "https://www.kennedy-center.org/whats-on/calendar/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    params = {"startDate": start_date, "endDate": end_date}
    try:
        # Use curl_cffi with browser impersonation like the original
        response = requests.get(url, headers=headers, params=params, impersonate="chrome120",
                                timeout=60, verify=False)
        if response.status_code != 200:
            return response.status_code, None
        else:
            data = list(response.json())
            return response.status_code, data
    except Exception as e:
        logging.error(f"Encountered error while calling api. error : {e}")
        return None, None


def get_list_of_events(url, start_date, end_date, max_retries=3):
    logging.info(f"Fetching events from Kennedy Center from {start_date} to {end_date}")
    retries = 0
    exception_msg = ""
    
    while retries < max_retries:
        try:
            response = call_events_list_api(url, start_date, end_date)
            status_code = response[0]

            if status_code == 200:
                if retries > 0:
                    logging.info(f"Fetched list of events after {retries} retries")
                return response[1]

            elif status_code == 429:
                delay = random.randint(8, 12)  # Longer delay for rate limiting
                logging.warning(f"Received 429 Too Many Requests. Retrying after {delay} seconds...")
                time.sleep(delay)
                retries += 1

            elif status_code == 403:
                delay = random.randint(10, 15)  # Longer delay for forbidden
                logging.warning(f"Received 403 Forbidden. Retrying after {delay} seconds...")
                time.sleep(delay)
                retries += 1

            elif status_code == 500:
                delay = random.randint(5, 8)
                logging.warning(f"Received 500 Response. Retrying after {delay} seconds...")
                time.sleep(delay)
                retries += 1

            else:
                delay = random.randint(5, 8)
                logging.warning(
                    f"Failed to fetch events. Status code: {status_code}. Error: {response[1]}. Retrying after {delay} seconds..."
                )
                time.sleep(delay)
                retries += 1
                
        except Exception as e:
            exception_msg = str(e)
            delay = random.randint(5, 8)
            logging.error(f"Exception occurred while getting list of events. Error: {exception_msg}. Retrying after {delay} seconds...")
            time.sleep(delay)
            retries += 1
    else:
        logging.error(f"Failed to fetch events after {max_retries} retries.")
        raise Exception(f"Failed to fetch events after {max_retries} retries. Error: {exception_msg}")


def check_onsale_date(on_sale_date_str):
    try:
        # Parse the date and handle the timezone offset manually
        date_without_tz = datetime.strptime(on_sale_date_str[:-6], "%Y-%m-%dT%H:%M:%S")
        offset_hours = int(on_sale_date_str[-6:-3])
        offset_minutes = int(on_sale_date_str[-2:])
        timezone_offset = timedelta(hours=offset_hours, minutes=offset_minutes)

        # Adjust for the timezone
        given_date = date_without_tz - timezone_offset

        # Get the current date and time
        current_date = datetime.now()

        # Calculate the difference
        date_difference = given_date - current_date

        # Logic: If the sale date is before today, return True
        if date_difference < timedelta(days=0):
            return True
        # Otherwise, check if it is within the next five days
        elif timedelta(days=0) <= date_difference <= timedelta(days=5):
            return True
        else:
            return False
    except Exception as e:
        exception_msg = str(e)
        logging.error(f"Exception occurred while checking on sale date: {exception_msg}.")
        return None
