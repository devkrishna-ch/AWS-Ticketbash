
import json
import os
import pymysql
import boto3
from datetime import datetime
from typing import Dict, Any, List
from scraper_mapping import scrapers, ui_scrapers
import logging
from read_config import read_config
from email_notification import send_email

# Initialize SQS client
sqs = boto3.client('sqs')

# Get environment variables
config = read_config()

# Constants
DEFAULT_DB_PORT = 3306

DB_CONFIG = {
    "database": config.get("DB_NAME"),
    "port": int(config.get("DB_PORT", DEFAULT_DB_PORT)),
    "host": config.get("DB_HOST"),
    "password": config.get("DB_PASSWORD"),
    "user": config.get("DB_USER")
}

SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
FARGATE_SQS_QUEUE_URL = os.environ.get("FARGATE_SQS_QUEUE_URL")


# convert email_recipient into list
email_recipient = config.get("RecipientEmailIds")
if isinstance(email_recipient, str):
    email_recipient = email_recipient.split(';')

email_subject='Lister AWS Prod'


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function for processing events for listing.
    
    This function fetches unprocessed events from the database and sends them
    to appropriate SQS queues based on venue type (Lambda or Fargate runtime).
    
    Args:
        event: AWS Lambda event object (unused)
        context: AWS Lambda context object (unused)
        
    Returns:
        dict: Response object with statusCode and body
    """
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("Connected to MySQL database")

    except pymysql.MySQLError as e:
        print(f"Database connection failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Database connection failed: {e}")
        }

    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        # Execute the query
        query = "SELECT * FROM events_to_process WHERE status = 'active' AND is_listed = 0 AND is_being_processed=0 AND in_sqs=0"
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Found {len(rows)} active, unlisted events")
        except pymysql.MySQLError as e:
            print(f"Query failed: {e}")
            connection.close()
            return {
                'statusCode': 500,
                'body': json.dumps(f"Query failed: {e}")
            }

        # Send each row to SQS
        event_count =0
        for row in rows:
            # Convert datetime objects to strings (if any)
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                # Boolean values are already JSON-serializable, no conversion needed

            # Determine processing runtime based on venue mapping
            # Check if venue exists in standard scrapers (Lambda runtime)
            if 'venue_name' in row and row['venue_name'] in scrapers:
                row['scraper_func_name'] = scrapers[row['venue_name']]  # Set scraper function name
                row['runtime'] = 'lambda'  # Use Lambda runtime for standard scrapers
            # Check if venue exists in UI scrapers (Fargate runtime)
            elif 'venue_name' in row and row['venue_name'] in ui_scrapers:
                row['task_def'] = ui_scrapers[row['venue_name']]  # Set task definition
                row['runtime'] = 'fargate'  # Use Fargate runtime for UI scrapers
            else:
                # Skip events with unmapped venues
                logging.warning(f"Venue name {row['venue_name']} not found in scraper mapping")
                continue

            # Add metadata for processing
            row['env'] = 'prod'           # Environment identifier
            row['process_name'] = 'lister'  # Process type identifier
            if row['runtime'] == 'lambda':
                # Send message to SQS
                try:
                    cursor.execute("SELECT in_sqs FROM events_to_process WHERE event_id = %s",(row['event_id']))

                    in_sqs_result = cursor.fetchone()
                    if in_sqs_result and in_sqs_result['in_sqs'] == 0:
                        response = sqs.send_message(
                            QueueUrl=SQS_QUEUE_URL,
                            MessageBody=json.dumps(row)
                        )
                        # Update the event in the database to indicate its in SQS
                        try:
                            update_query = "UPDATE events_to_process SET in_sqs = 1 WHERE event_id = %s"
                            cursor.execute(update_query, (row['event_id']))
                            connection.commit()
                            print(f"Updated event {row['event_id']} in database")

                        except pymysql.MySQLError as e:
                            print(f"Failed to update event in database: {e}")

                        print(f"Sent message to SQS: {response['MessageId']}")
                        event_count += 1
                except Exception as e:
                    print(f"Failed to send to SQS: {e}")
            else:
                # Send message to Fargate SQS
                try:
                    cursor.execute("SELECT in_sqs FROM events_to_process WHERE event_id = %s", (row['event_id'],))
                    in_sqs_result = cursor.fetchone()
                    if in_sqs_result and in_sqs_result['in_sqs'] == 0:
                        response = sqs.send_message(
                        QueueUrl=FARGATE_SQS_QUEUE_URL,
                        MessageBody=json.dumps(row)
                        )
                        try:

                            update_query = "UPDATE events_to_process SET in_sqs = 1 WHERE event_id = %s"
                            cursor.execute(update_query, (row['event_id']))
                            connection.commit()
                            print(f"Updated event {row['event_id']} in database")
                            print(f"Sent message to Fargate SQS: {response['MessageId']}")
                        except pymysql.MySQLError as e:
                            print(f"Failed to update event in database: {e}")

                        event_count += 1


                except Exception as e:
                    print(f"Failed to send to Fargate SQS: {e}")

    # Clean up
    connection.close()
    print("Database connection closed")
    
    # Send email notification with processing summary
    body_data = {
             "text": (
                   "Hi team,\n\n"
                   "Event has been processed. Once listing is done, you will receive details in an email.\n\n"
                   f"Processed {len(rows)} events and sent {event_count} to SQS."
                ),
             "html": (
                    f"<p>Hi team,</p>"
                    f"<p>Event has been processed. Once listing is done, you will receive details in an email.</p>"
                    f"<p>Processed <strong>{len(rows)}</strong> events and sent <strong>{event_count}</strong> to SQS.</p>"
                )
            }

    # Send notification email to configured recipients
    send_email(list(email_recipient) if email_recipient else [], email_subject, body_data)
    # Return success response
    
    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {len(rows)} events and sent to SQS")
    }
