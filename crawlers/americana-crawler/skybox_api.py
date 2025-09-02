import os

from curl_cffi import requests
import json
import time
from read_config import read_config
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_event(venue_name, exclude_active_inventory, event_date_from, event_date_to, config, max_retries):
    SKYBOX_API_TOKEN = config.get('Skybox_APIToken')
    SKYBOX_APP_TOKEN = config.get('Skybox_APIAppToken')
    SKYBOX_ACCOUNT = os.getenv('SKYBOX_ACCOUNT')
    SKYBOX_ENDPOINT_GET_EVENT = config.get('Skybox_APIGetEvent_EndPoint')
    SKYBOX_ENDPOINT_GET_INVENTORY = config.get('SkyBox_GetInventoryEndPoint')
    SKYBOX_ENDPOINT_PURCHASE_INVENTORY = config.get('Skybox_PurchaseAPI_EndPoint')
    logging.info(f"Fetching events from {venue_name} from {event_date_from} to {event_date_to}")
    url = SKYBOX_ENDPOINT_GET_EVENT
    retries = 0

    headers = {
        'X-Api-Token': SKYBOX_API_TOKEN,
        'X-Application-Token': SKYBOX_APP_TOKEN,
        'X-Account': SKYBOX_ACCOUNT,
        'Content-Type': 'application/json',
    }
    params = {
        'venue': venue_name,
        'excludeParking': "True",
        'excludeActiveInventory': exclude_active_inventory,
        'eventDateFrom': event_date_from,
        'eventDateTo': event_date_to,
    }
    while retries < max_retries:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=60)
            if response.status_code == 200:
                if retries > 0:
                    logging.info(f"SkyboxEvents fetched successfully after {retries}.")
                else:
                    logging.info("SkyboxEvents fetched successfully.")
                   
                return response.json()

            else:
                logging.error(f"Error fetching events: status code:{response.status_code}.")
                if retries < max_retries:
                    logging.info(f"Retrying...")
                retries += 1

            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching events: {e}")
            retries += 1
            if retries < max_retries:
                logging.info(f"Retrying... (Attempt {retries + 1} of {max_retries})")
                time.sleep(2)  # Wait for 2 seconds before retrying
    else:
        logging.error("Max retries reached. Unable to fetch skybnox events.")
        return None

                


def get_inventory(event_id):
    url = SKYBOX_ENDPOINT_GET_INVENTORY
    headers = {
        'X-Api-Token': API_KEY,
        'X-Application-Token': APP_TOKEN,
        'X-Account': SKYBOX_ACCOUNT,
        'Content-Type': 'application/json',
    }

    params = {
        'eventId': event_id,
    }

    response = requests.get(url, params=params, headers=headers, timeout=60)
    return response.json()


def delete_inventory():
    pass


def create_purchase_entry(df_purchase_data, venue_id, event_id, request_body):
    url = SKYBOX_ENDPOINT_PURCHASE_INVENTORY
    vendor_id = ""
    headers = {
        'X-Api-Token': API_KEY,
        'X-Application-Token': APP_TOKEN,
        'X-Account': SKYBOX_ACCOUNT,
        'Content-Type': 'application/json',
    }
    
    response = requests.post(url, headers=headers, data=request_body, timeout=60)
    return response.json()


def update_inventory_price(url, price, inventory_id, broadcast):
    body = [
        {
            "listPrice": str(price),
            "broadcast": str(broadcast),
            "id": str(inventory_id)
        }
    ]
    headers = {
        'X-Api-Token': API_KEY,
        'X-Application-Token': APP_TOKEN,
        'X-Account': SKYBOX_ACCOUNT,
    }
    pass