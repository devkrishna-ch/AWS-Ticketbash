import json
import os
import time
import logging
import random
import io
import uuid
from datetime import datetime
from read_config import read_config
# Import curl cffi
import requests

# === CONFIGURATION ===
config = read_config()
client_id = config.get('ORCHESTRATOR_APP_ID')
client_secret = config.get('ORCHESTRATOR_APP_SECRET')
account_logical_name = config.get('ACCOUNT_NAME')
tenant_logical_name = config.get('TENANT_NAME')
organization_unit_id = config.get('ORGANIZATION_UNIT_ID')
bucket_id = config.get('BUCKET_ID')

# -- for testing
# organization_unit_id = "313831"
# bucket_id = "23555"

queue_name = {
    'lister': config.get('LISTER_QUEUE_NAME'),
    'checker': config.get('CHECKER_QUEUE_NAME')
} 
orchestrator_url = f'https://cloud.uipath.com/{account_logical_name}/{tenant_logical_name}'



# Configure logging
logger = logging.getLogger(__name__)

# Implement enhanced retry logic for curl cffi requests with focus on throttling
def make_request_with_retry(method, url, headers=None, data=None, json_data=None, 
                           max_retries=5, initial_backoff=1.0, 
                           backoff_factor=2.0, jitter=0.1,
                           status_forcelist=(429, 500, 502, 503, 504)):
    """
    Make a request with enhanced retry logic for throttling using curl_cffi
    
    Args:
        method: HTTP method (get, post)
        url: Request URL
        headers: Request headers
        data: Form data for POST requests
        json_data: JSON data for POST requests
        max_retries: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
        backoff_factor: Multiplier for backoff time on each retry
        jitter: Random jitter factor to add to backoff (0.1 = 10%)
        status_forcelist: Status codes that trigger a retry
    """
    headers = headers or {}
    retries = 0
    
    while retries <= max_retries:
        try:
            if method.lower() == 'get':
                response = requests.get(url, headers=headers)
            elif method.lower() == 'post':
                if json_data:
                    response = requests.post(url, headers=headers, json=json_data)
                else:
                    response = requests.post(url, headers=headers, data=data)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            # Check for throttling (429) specifically
            if response.status_code == 429:
                # Try to get retry-after header
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        # If Retry-After is in seconds
                        wait_time = float(retry_after)
                    except ValueError:
                        # If Retry-After is a date
                        wait_time = initial_backoff * (backoff_factor ** retries)
                else:
                    # Use exponential backoff if no Retry-After header
                    wait_time = initial_backoff * (backoff_factor ** retries)
                
                # Add jitter to prevent synchronized retries
                jitter_amount = wait_time * jitter * random.uniform(-1, 1)
                wait_time = max(0.1, wait_time + jitter_amount)
                
                logger.warning(f"Request throttled (429), waiting {wait_time:.2f}s before retry {retries+1}/{max_retries}")
                time.sleep(wait_time)
                retries += 1
                continue
            
            # For other status codes in forcelist
            if response.status_code in status_forcelist:
                wait_time = initial_backoff * (backoff_factor ** retries)
                jitter_amount = wait_time * jitter * random.uniform(-1, 1)
                wait_time = max(0.1, wait_time + jitter_amount)
                
                logger.warning(f"Request failed with status {response.status_code}, waiting {wait_time:.2f}s before retry {retries+1}/{max_retries}")
                time.sleep(wait_time)
                retries += 1
                continue
            
            # If we get here, the request was successful or failed with a non-retryable status code
            return response
                
        except Exception as e:
            wait_time = initial_backoff * (backoff_factor ** retries)
            jitter_amount = wait_time * jitter * random.uniform(-1, 1)
            wait_time = max(0.1, wait_time + jitter_amount)
            
            logger.warning(f"Request failed with exception: {str(e)}, waiting {wait_time:.2f}s before retry {retries+1}/{max_retries}")
            time.sleep(wait_time)
            retries += 1
    
    # If we've exhausted retries, make one final attempt and let any exceptions propagate
    if method.lower() == 'get':
        return requests.get(url, headers=headers)
    elif method.lower() == 'post':
        if json_data:
            return requests.post(url, headers=headers, json=json_data)
        else:
            return requests.post(url, headers=headers, data=data)

# === STEP 1: GET ACCESS TOKEN ===
def get_access_token():
    token_url = f'https://cloud.uipath.com/identity_/connect/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'OR.Administration OR.Queues'
    }

    response = make_request_with_retry('post', token_url, headers=headers, data=data)
    print(f"Response: {response.status_code} - {response.text}")
    response.raise_for_status()
    return response.json()['access_token']

# === STEP 2: GET QUEUE ID ===
def get_queue_id(access_token, process):
    url = f'{orchestrator_url}/odata/Queues?$filter=Name eq \'{queue_name[process]}\''
    headers = {'Authorization': f'Bearer {access_token}'}
    res = make_request_with_retry('get', url, headers=headers)
    res.raise_for_status()
    queues = res.json()['value']
    if not queues:
        raise Exception(f"Queue '{queue_name[process]}' not found.")
    return queues[0]['Id']

# === STEP 3: ADD QUEUE ITEM WITH RETRY LOGIC ===
def add_queue_item(access_token, result, process):
    """
    Adds a queue item to the UiPath Orchestrator queue with retry logic.
    """
    url = f'{orchestrator_url}/odata/Queues/UiPathODataSvc.AddQueueItem'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'X-UIPATH-OrganizationUnitId': organization_unit_id
    }

    payload = {
        "itemData": {
            "Name": queue_name[process],
            "Priority": "Normal",
            "SpecificContent": result,
            "Reference": f"{result['venue_name']}_{result['venue_id']}"
        }
    }

    try:
        # Use the retry logic for the POST request
        response = make_request_with_retry('post', url, headers=headers, json_data=payload)
     
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to add queue item: {str(e)}")
        raise

def add_item_to_queue(result, process):
    """
    Wrapper function to get the access token and add a queue item with retry logic.
    """
    try:
        access_token = get_access_token()
        return add_queue_item(access_token, result, process)
    except Exception as e:
        logger.error(f"Failed to add item to queue for process '{process}': {str(e)}")
        raise

def get_write_uri(access_token, bucket_name, file_name):
    """
    Retrieves a pre-signed WriteUri for uploading a file to a UiPath Orchestrator bucket.
    """
    url = f'{orchestrator_url}/odata/Buckets({bucket_id})/UiPath.Server.Configuration.OData.GetWriteUri?path={file_name}'

    print(f"URL: {url}")
    print(f"BucketID: {bucket_id}")
    print(f"File Name: {file_name}")
    print(f"Organization Unit ID: {organization_unit_id}")
    logger.info(f"URL: {url}")
    headers = {
        'Authorization': f'Bearer {access_token}',
        'X-UIPATH-OrganizationUnitId': organization_unit_id,
        'Content-Type': 'application/json'
    }

    response = make_request_with_retry('get', url, headers=headers) 
    
    response.raise_for_status()
    return response.json()['Uri']

def upload_json_content_to_bucket(access_token, bucket_name, json_content, file_name):
    """
    Uploads JSON content to a UiPath Orchestrator bucket.
    """
    event_data = {
        "event_data": json_content
    }
    # Get the write URI
    write_uri = get_write_uri(access_token, bucket_name, file_name)
    headers = {
    'x-ms-blob-type': 'BlockBlob',
    'Content-Type': 'application/json',
    }
    # Convert JSON content to bytes and upload
    compact_json = json.dumps(event_data, separators=(',', ':'))
    with io.BytesIO(compact_json.encode('utf-8')) as file:
        response = requests.put(write_uri, data=file, headers=headers)
        response.raise_for_status()

    return f"{bucket_name}/{file_name}"  # Return the bucket file path

def add_queue_item_with_bucket(access_token, result, process, bucket_name):
    """
    Uploads JSON content to a bucket and adds a queue item with the bucket file path.
    """
    # Step 1: Extract event_data and upload to bucket
    event_data = result.pop('event_data', None)  # Remove event_data from result
   
    if event_data is not None: 
        # Generate a unique file name using event_id, timestamp, and a UUID
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        unique_id = uuid.uuid4().hex[:8]  # Shortened UUID for brevity
        file_name = f"{result.get('venue_id', 'event_data')}_{result.get('event_id', 'unknown')}_{timestamp}_{unique_id}.json"
        bucket_file_path = upload_json_content_to_bucket(access_token, bucket_name, event_data, file_name)
        
        # Step 2: Add the bucket file path to SpecificContent
        result['file_path'] = bucket_file_path

    # Step 3: Add the queue item
    url = f'{orchestrator_url}/odata/Queues/UiPathODataSvc.AddQueueItem'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'X-UIPATH-OrganizationUnitId': organization_unit_id
    }

    payload = {
        "itemData": {
            "Name": queue_name[process],
            "Priority": "Normal",
            "SpecificContent": result,
            "Reference": f"{result['venue_name']}_{result['venue_id']}_{result['event_id']}"
        }
    }

    try:
        # Use the retry logic for the POST request
        response = make_request_with_retry('post', url, headers=headers, json_data=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to add queue item: {str(e)}")
        raise

def add_item_to_queue_with_bucket(result, process, bucket_name):
    """
    Wrapper function to get the access token, upload JSON to bucket, and add a queue item.
    """
    try:
        access_token = get_access_token()
        return add_queue_item_with_bucket(access_token, result, process, bucket_name)
    except Exception as e:
        logger.error(f"Failed to add item to queue for process '{process}': {str(e)}")
        raise

# === MAIN FLOW ===
if __name__ == '__main__':
    try:
        # Example: Upload event_data and add to queue
        bucket_name = 'example-bucket-name'  # Replace with your bucket name
        process = 'checker'  # Replace with the process name (e.g., 'lister' or 'checker')

        # Example result object
        result = {
            "event_id": "85645",
            "venue_name": "Kennedy Center",
            "venue_id": "12345",
            "status": "success",
            "reason": "Completed",
            "event_data": {
                "key1": "value1",
                "key2": "value2"
            }
        }

        print("📤 Uploading JSON content and adding item to queue...")
        response = add_item_to_queue_with_bucket(result, process, bucket_name)
        print("✅ Successfully added queue item:")
        print(response)

    except Exception as e:
        print("Failed:", str(e))
