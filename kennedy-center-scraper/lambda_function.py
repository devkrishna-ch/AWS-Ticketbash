import json
import os
import pandas as pd
from kennedy_center_scraper import check_event
from read_config import read_config
from sqlalchemy import create_engine, Table, MetaData, update
from datetime import datetime, timedelta
import logging
from orchestrator_api import add_item_to_queue_with_bucket
import boto3
from skybox_api import get_inventory

def log_error_to_db(engine, venue_name=None, venue_id=None, event_name=None, 
                   event_id=None, event_date=None, event_time=None, 
                   error_details=None, process_name=None):
    """Log error to the errors table using SQLAlchemy"""
    try:
        error_data = [{
            'venue_name': venue_name,
            'venue_id': venue_id,
            'event_name': event_name,
            'event_id': event_id,
            'event_date': event_date,
            'event_time': event_time,
            'error_details': error_details,
            'timestamp': datetime.now(),
            'process_name': process_name
        }]

        df = pd.DataFrame(error_data)
        df.to_sql("errors", engine, if_exists="append", index=False)

        logging.info(f"Error logged to database: {error_details[:100]}...")

    except Exception as e:
        logging.error(f"Failed to log error to database: {e}")

def lambda_handler(event, context):
    engine = None
    try:
        #reading configuration
        config = read_config()
        event_body = event.get('parsed','')
        logging.info(f"Scraping for event id: {event_body.get('event_id')}")

        # Retrieve DB credentials from environment variables
        DB_HOST = config.get('DB_HOST')
        DB_USER = config.get('DB_USER')
        DB_PASSWORD = config.get('DB_PASSWORD')
        DB_NAME = config.get('DB_NAME')
        DB_PORT = config.get('DB_PORT', 3306)

        # Create engine early for error logging
        engine_url = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(engine_url)

        event_api_url = config.get('Kennedy_Center_EventAPI_URL')
        seatmap_id_url = config.get('Kennedy_Center_SeatMapId_URL')
        seatmap_url = config.get('Kennedy_Center_SeatMap_URL')
        
        event_date_time = datetime.strptime(event_body.get('event_datetime'), "%Y-%m-%d %H:%M:%S")
        event_id = event_body.get('event_unique_id',"82888")
        start_date = event_date_time.strftime("%m-%d-%Y")
        next_day = event_date_time + timedelta(days=1)
        end_date = next_day.strftime("%m-%d-%Y")
        skybox_event_id = event_body.get('event_id', "")
        process_name = event_body.get('process_name', "")

        # Extract separate date and time for error logging
        evt_date = event_date_time.strftime("%Y-%m-%d")
        evt_time = event_date_time.strftime("%H:%M:%S")

        venue_name = event_body.get('venue_name')
        event_name = event_body.get('event_name')
        venue_id = event_body.get('venue_id')
        event_identifier = event_body.get('event_unique_id')

        bucket_name = config.get('BucketName','')

        # Execute scraping with error handling
        try:
            result = check_event(
                seatmap_url,
                seatmap_id_url,
                event_identifier
            )
        except Exception as scrape_error:
            error_msg = f"Kennedy Center scraping failed: {str(scrape_error)}"
            log_error_to_db(engine, venue_name, venue_id, event_name, skybox_event_id, 
                          evt_date, evt_time, error_msg, process_name)
            raise Exception(error_msg)

        # Parse scraper output
        try:
            output = json.loads(result)
        except json.JSONDecodeError as parse_error:
            error_msg = f"Could not parse scraper output: {parse_error}"
            log_error_to_db(engine, venue_name, venue_id, event_name, skybox_event_id,
                          evt_date, evt_time, error_msg, process_name)
            raise Exception(error_msg)
        
        # Initialize df as None
        df = None
        
        if output.get("status") == "success":
            df = pd.DataFrame(output.get("event_data"))
            #renaming columns in the df to match DB table columns
            df.rename(columns={
                    "Venue Name": "venue_name",
                    "Event Name": "event_name",
                    "Event Date": "event_date",
                    "Event Time": "event_time",
                    "Section": "section",
                    "Row": "row",
                    "Seat": "seat_no",
                    "Price": "price",
                    "Desc": "description",
                    "UniqueIdentifier": "unique_id",
                    "TimeStamp": "timestamp",
                    "Seat Type": "seat_type"
                }, inplace=True)

            # converting event_data to string to store in orchestrator queue
            output['event_data'] = df.to_dict(orient='records')
        else:
            error_msg = f"Event not available or no data returned"
            logging.info(error_msg)
            log_error_to_db(engine, venue_name, venue_id, event_name, skybox_event_id,
                  evt_date, evt_time, error_msg, process_name)
             
        logging.info(f"{output}")
        output['event_id']= skybox_event_id
        output['venue_name']= venue_name
        output['event_name']= event_name
        output['venue_id'] = venue_id
        output['event_date'] = evt_date
        output['event_time'] = evt_time

        logging.info(f"output bucket: {output}")

        if process_name == "lister" or process_name == "checker":
            # send api call to orchestrator to trigger lister process
            add_item_to_queue_with_bucket(output, process_name, bucket_name)

            # Only insert to database if we have data
            if df is not None and not df.empty:
                # Insert the DataFrame into the DB table 
                df.to_sql('scraper_data', con=engine, if_exists='append', index=False)
                logging.info(f"Added {len(df)} rows to scraper_data table")
            else:
                logging.info("No seat data to insert into database")

            # Always update events_to_process regardless of seat data
            metadata = MetaData()
            metadata.reflect(bind=engine)
            events_to_process = metadata.tables['events_to_process']

            # Create the update statement
            stmt = (
                update(events_to_process)
                .where(events_to_process.c.event_id == output.get('event_id'))
                .values(is_being_processed=1)
            )

            # Execute the update
            with engine.begin() as conn:
                conn.execute(stmt)
        else:
            error_msg = "Invalid process name"
            log_error_to_db(engine, venue_name, venue_id, event_name, skybox_event_id,
                          evt_date, evt_time, error_msg, process_name)
            raise Exception(error_msg)

        return{
            'statusCode': 200,
            'message':  f"Scraping for event id: {event_body.get('event_id')} completed"
        }

    except Exception as e:
        logging.exception("Kennedy Center scraper failed with exception.")
        
        # Log the main exception to database
        if 'engine' in locals() and engine:
            log_error_to_db(engine,
                venue_name=event_body.get('venue_name', 'Kennedy Center') if 'event_body' in locals() else 'Kennedy Center',
                venue_id=str(event_body.get('venue_id', '')) if 'event_body' in locals() else '',
                event_name=event_body.get('event_name', '') if 'event_body' in locals() else '',
                event_id=str(event_body.get('event_id', '')) if 'event_body' in locals() else '',
                event_date=evt_date if 'evt_date' in locals() else None,
                event_time=evt_time if 'evt_time' in locals() else None,
                error_details=str(e),
                process_name=event_body.get('process_name', '') if 'event_body' in locals() else ''
            )

        # Create error payload for orchestrator
        error_payload = {
            "status": "error",
            "reason": str(e),
            "error": str(e),
            "event_id": event_body.get("event_id", "") if 'event_body' in locals() else "",
            "venue_id": event_body.get("venue_id", 0) if 'event_body' in locals() else 0,
            "venue_name": event_body.get("venue_name", "Kennedy Center") if 'event_body' in locals() else "Kennedy Center",
            "event_name": event_body.get("event_name", "") if 'event_body' in locals() else "",
            "event_date" : event_body.get("event_date", "") if 'event_body' in locals() else "",
            "event_time" : event_body.get("event_time", "") if 'event_body' in locals() else "",
            "event_data": []
        }

        # Enqueue error payload if process specified
        if 'event_body' in locals() and event_body.get("process_name") in ("lister", "checker"):
            try:
                add_item_to_queue_with_bucket(error_payload, event_body["process_name"], config.get('BucketName',''))
                logging.info(f"Error payload enqueued to {event_body['process_name']}")
            except Exception as qerr:
                logging.warning(f"Queue push for error payload failed: {qerr}")

        return {
            "statusCode": 500,
            "body": json.dumps(error_payload),
            "headers": {"Content-Type": "application/json"},
        }
    finally:
        if engine:
            engine.dispose()
            logging.info("SQLAlchemy engine disposed.")
