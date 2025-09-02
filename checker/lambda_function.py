"""
AWS Lambda function for processing events and sending them to SQS queues.

This function queries a database for events based on specified criteria,
maps venues to appropriate scrapers, and sends qualifying events to
either Lambda or Fargate SQS queues for processing.
"""

import json
import os
import pymysql
import boto3
from datetime import datetime, timedelta
from scraper_mapping import scrapers, ui_scrapers
import logging
from read_config import read_config
from typing import Dict, Any, List, Optional
from email_notification import send_email

# Initialize SQS client
sqs = boto3.client('sqs')
config = read_config()

# Database configuration from S3 config
DB_CONFIG = {
    "database": config.get("DB_NAME"),
    "port": int(config.get("DB_PORT", "3306")),
    "host": config.get("DB_HOST"),
    "password": config.get("DB_PASSWORD"),
    "user": config.get("DB_USER")
}

# SQS Queue URLs from environment variables
EMAIL_SQS_QUEUE_URL = os.environ.get("EMAIL_SQS_QUEUE_URL")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL") # Lambda runtime queue
FARGATE_SQS_QUEUE_URL = os.environ.get("FARGATE_SQS_QUEUE_URL") # Fargate runtime queue
 
#email
# convert email_recipient into list
email_recipient = config.get("RecipientEmailIds")
if isinstance(email_recipient, str):
    email_recipient = email_recipient.split(';')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function.
    
    Args:
        event: Lambda event containing query parameters and configuration
        context: Lambda context object
        
    Returns:
        Dict containing status code and response body
    """
    logging.info(f"Received event: {event}")

    if not SQS_QUEUE_URL or not FARGATE_SQS_QUEUE_URL or not EMAIL_SQS_QUEUE_URL:
        raise RuntimeError("Missing required SQS Queue URL env vars")
    if not DB_CONFIG:
        raise RuntimeError("Missing required DB config")
    # Extract parameters from event
    params = event['queryParams']
    print(params)
    table_name = event['tableName']  # Database table to query
    is_Listed = params.get('isListed')
    is_being_processed = params.get('isBeingProcessed')
    in_sqs = params.get('inSqs')
    status = params.get('status')
    event_startdays = params['eventDatetimeRange'].get('start')
    event_enddays = params['eventDatetimeRange'].get('end')
    email_subject = event['emailSubject']
    
    # Initialize database connection
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("Connected to MySQL database")
    except pymysql.MySQLError as e:
        print(f"Database connection failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Database connection failed: {e}")
        }

    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Build and execute parameterized query to prevent SQL injection

            # Prepare the first parameter conditionally
            event_range = (
                          f"{event_enddays}" if event_startdays == "" else f"{event_startdays} - {event_enddays}"
                          )
            print("day range:", event_range)
           
           # update date_range column in db for further refrence
            query = f"""
                  UPDATE {table_name}
                  SET day_range = %s
                    WHERE status = %s
                    AND is_listed = %s
                    AND is_being_processed = %s
                    AND in_sqs = %s
                """
            cursor.execute(query, (event_range, status, is_Listed, is_being_processed, in_sqs))
                

            if event_startdays == "":
                # Query for events from event_end onwards (no start date)
                event_end = (datetime.now() + timedelta(days=int(event_enddays))).strftime('%Y-%m-%d')
                query = f"""
                SELECT * FROM {table_name}
                WHERE status = %s
                AND is_listed = %s
                AND is_being_processed = %s
                AND in_sqs = %s
                AND event_datetime >= %s
                """
                
                cursor.execute(query, (status, is_Listed, is_being_processed, in_sqs, event_end))
            else:
                # Query for events within date range
                event_end = (datetime.now() + timedelta(days=int(event_enddays))).strftime('%Y-%m-%d')
                event_start = (datetime.now() + timedelta(days=int(event_startdays))).strftime('%Y-%m-%d')
                query = f"""
                SELECT * FROM {table_name}
                WHERE status = %s
                AND is_listed = %s
                AND is_being_processed = %s
                AND in_sqs = %s
                AND event_datetime BETWEEN %s AND %s
                """
                cursor.execute(query, (status, is_Listed, is_being_processed, in_sqs, event_start, event_end))
            
            rows = cursor.fetchall()
            print(f"Found {len(rows)} events")
            
            events_sent_to_sqs = 0
            
            # Process each event row
            for row in rows:
                # Convert datetime objects to strings for JSON serialization
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                
                # Map venue to appropriate scraper and runtime
                if 'venue_name' in row and row['venue_name'] in scrapers:
                    # Lambda runtime scraper
                    row['scraper_func_name'] = scrapers[row['venue_name']]
                    row['runtime'] = 'lambda'
                elif 'venue_name' in row and row['venue_name'] in ui_scrapers:
                    # Fargate runtime scraper (for UI-based scrapers)
                    row['task_def'] = ui_scrapers[row['venue_name']]
                    row['runtime'] = 'fargate'
                else:
                    # Skip events with unmapped venues
                    logging.warning(f"Venue name {row['venue_name']} not found in scraper mapping")
                    continue
                
                # Add processing metadata
                row['env'] = 'prod'
                row['process_name'] = 'checker'
                
                # Check if event is already in SQS to avoid duplicates
                cursor.execute(f"SELECT in_sqs FROM {table_name} WHERE event_id = %s", (row['event_id'],))
                in_sqs_result = cursor.fetchone()
                
                # Only process events not already in SQS
                if in_sqs_result and in_sqs_result['in_sqs'] == 0:
                    try:
                        # Send to appropriate SQS queue based on runtime
                        queue_url = SQS_QUEUE_URL if row['runtime'] == 'lambda' else FARGATE_SQS_QUEUE_URL
                        response = sqs.send_message(
                            QueueUrl=queue_url,
                            MessageBody=json.dumps(row)
                        )
                        
                        # Mark event as sent to SQS in database
                        update_query = f"UPDATE {table_name} SET in_sqs = 1 WHERE event_id = %s"
                        cursor.execute(update_query, (row['event_id'],))
                        connection.commit()
                        
                        print(f"Updated event {row['event_id']} and sent to {row['runtime']} SQS: {response['MessageId']}")
                        events_sent_to_sqs += 1
                        
                    except Exception as e:
                        print(f"Failed to send event {row['event_id']} to SQS: {e}")
                        connection.rollback()

        # Send email notification with processing summary
        body_data = {
             "text": (
                   "Hi team,\n\n"
                   "Event has been processed. Once checking is done, you will receive details in an email.\n\n"
                   f"Processed {len(rows)} events and sent {events_sent_to_sqs} to SQS."
                ),
             "html": (
                    f"<p>Hi team,</p>"
                    f"<p>Event has been processed. Once checking is done, you will receive details in an email.</p>"
                    f"<p>Processed <strong>{len(rows)}</strong> events and sent <strong>{events_sent_to_sqs}</strong> to SQS.</p>"
                )
            }

        send_email(email_recipient, email_subject, body_data)

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps(f"Processed {len(rows)} events and sent {events_sent_to_sqs} to SQS")
        }

    except pymysql.MySQLError as e:
        print(f"Query failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Query failed: {e}")
        }
    finally:
        # Ensure database connection is always closed
        if connection:
            connection.close()
            print("Database connection closed")
