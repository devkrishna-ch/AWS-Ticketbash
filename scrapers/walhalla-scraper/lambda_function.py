import json
import ast
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, MetaData, update
from read_config import read_config
from orchestrator_api import add_item_to_queue_with_bucket
from walhalla_scraper import scrape_event
from error_logger import log_error_to_db

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = read_config()
DB_NAME     = config.get("DB_NAME")
DB_PORT     = config.get("DB_PORT")
DB_HOST     = config.get("DB_HOST")
DB_PASSWORD = config.get("DB_PASSWORD")
DB_USER     = config.get("DB_USER")
bucket_name = config.get("BucketName", "")

def save_eventData_to_db(response_body: str) -> None:
    payload = json.loads(response_body)
    rows = payload.get("event_data", [])
    logger.info("saved walhalla_Events event_data sample: %s", rows[:1])

    df = pd.DataFrame(rows)
    if df.empty:
        logger.info("DataFrame is empty — no seats to persist.")
        return

    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"], format="%Y-%m-%d", errors="coerce")

    if "event_time" in df.columns:
        def _norm(t):
            try:
                return datetime.strptime(t, "%H:%M:%S").strftime("%H:%M:%S")
            except Exception:
                return None
        df["event_time"] = df["event_time"].astype(str).apply(_norm)

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "row" in df.columns:
        df["row"] = df["row"].astype(str).str.replace(r"^Row:\s*", "", regex=True).str.strip()

    if "seat_no" in df.columns:
        df["seat_no"] = pd.to_numeric(df["seat_no"], errors="coerce").fillna(0).astype(int)

    # Add timestamp processing - handle after column renaming
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d %b %Y %H:%M:%S", errors="coerce")
        df["timestamp"] = df["timestamp"].fillna(pd.Timestamp.now())

    if "uniqueidentifier" in df.columns:
        df = df.rename(columns={"uniqueidentifier": "unique_id"})

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    try:
        df.to_sql("scraper_data", engine, if_exists="append", index=False, chunksize=500)
        logger.info("saved %s rows to scraper_data table", len(df))
    except Exception as e:
        logger.exception("Failed to write to MySQL: %s", e)

        venue_name = None
        venue_id = None
        event_name = None
        event_id = None
        event_date = None
        event_time = None

        if not df.empty:
            first_row = df.iloc[0].to_dict()
            venue_name = first_row.get("venue_name")
            event_name = first_row.get("event_name") 
            event_id = first_row.get("event_id")
            event_date = first_row.get("event_date")
            event_time = first_row.get("event_time")

        log_error_to_db(engine, 
           venue_name=venue_name,
           venue_id=venue_id,
           event_name=event_name,
           event_id=event_id,
           event_date=event_date,
           event_time=event_time,
           error_details=str(e), 
           process_name="save_eventData_to_db")

def lambda_handler(event, context):
    logger.info("lambda_handler invoked with event = %s", event)

    engine = None
    venue_name = "Walhalla Performing Arts Center"
    evt_name = ""
    evt_date = ""
    evt_time = ""
    process = ""
    event_num = None
    venue_id = None

    try:
        body = event.get("parsed", {})
        process = body.get("process_name", "")

        event_unique_id = body["event_unique_id"]
        skybox_id = body["event_id"]
        venue_id = int(body.get("venue_id", 0))
        venue_name = body.get("venue_name", "Walhalla Performing Arts Center")
        evt_name = body.get("event_name", "")
        evt_date, evt_time = body["event_datetime"].split(" ")
        event_url = body.get("event_url", "")

        engine_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(engine_url)

        # Try scraping the event data and handling real scrape exceptions
        try:
            walhalla_Events = scrape_event(event_url, event_unique_id, venue_name)
            try:
                out = json.loads(walhalla_Events)
            except json.JSONDecodeError:
                out = ast.literal_eval(walhalla_Events)
            data = out.get("event_data", [])
        except Exception as e:
            logger.error("Scrape failed: %s", e)
            event_num = int(str(skybox_id).split("_")[-1]) if "_" in str(skybox_id) else int(skybox_id)

            # Create error payload for downstream processing
            payload = {
                "reason": str(e),  # Include error message
                "status": "error",  # Mark as error status
                "event_id": event_num,  # Numeric event ID
                "venue_id": venue_id,  # Venue identifier
                "venue_name": venue_name,  # Venue name
                "event_name": evt_name,  # Event name
                "event_date": evt_date,  # Event date
                "event_time": evt_time,  # Event time
                "event_data": []  # Empty data array for failed scrape
            }
            # Log the scraping error to database for monitoring
            log_error_to_db(engine, venue_name=venue_name, venue_id=str(venue_id), event_name=evt_name,
                            event_id=str(event_num), event_date=evt_date, event_time=evt_time,
                            error_details=str(e), process_name=process)

            # For lister/checker processes, enqueue error payload for retry/handling
            if process in ("lister", "checker"):
                try:
                    add_item_to_queue_with_bucket(payload, process, bucket_name)  # Add to processing queue
                except Exception as qe:
                    logger.error("Failed to enqueue error payload: %s", qe)  # Log queue failure
                    # Log the queue error as well
                    log_error_to_db(engine, venue_name=venue_name, venue_id=str(venue_id), event_name=evt_name,
                                    event_id=str(event_num), event_date=evt_date, event_time=evt_time,
                                    error_details=str(qe), process_name=process)

            return {
                "statusCode": 200,
                "body": payload,
                "headers": {"Content-Type": "application/json"}
            }

        event_num = int(str(skybox_id).split("_")[-1]) if "_" in str(skybox_id) else int(skybox_id)

        # Handle no seat data after scrape
        if not data:
            payload = {
                "reason": "No seat data found.",
                "status": "error",
                "event_id": event_num,
                "venue_id": venue_id,
                "venue_name": venue_name,
                "event_name": evt_name,
                "event_date": evt_date,
                "event_time": evt_time,
                "error": "No seat data found."
            }
            log_error_to_db(engine, venue_name=venue_name, venue_id=str(venue_id), event_name=evt_name,
                            event_id=str(event_num), event_date=evt_date, event_time=evt_time,
                            error_details="No seat data found.", process_name=process)

            if process in ("lister", "checker"):
                try:
                    add_item_to_queue_with_bucket(payload, process, bucket_name)
                    logger.info("Enqueued error payload into %s queue", process)
                except Exception as e:
                    logger.error("Failed to enqueue error payload: %s", e)
                    log_error_to_db(engine, venue_name=venue_name, venue_id=str(venue_id), event_name=evt_name,
                                    event_id=str(event_num), event_date=evt_date, event_time=evt_time,
                                    error_details=str(e), process_name=process)

            return {
                "statusCode": 200,
                "body": payload,
                "headers": {"Content-Type": "application/json"}
            }

        # Process data
        payload = {
            "reason": "success",
            "status": "success",
            "event_id": event_num,
            "venue_id": venue_id,
            "event_data": data,
            "venue_name": venue_name,
            "event_name": evt_name,
            "event_date": evt_date,
            "event_time": evt_time,
        }

        df = pd.DataFrame(data)

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

        payload["event_data"] = df.to_dict(orient="records")

        # ─── Queue Management & Database Updates for Lister/Checker Processes ────────────────────
        if process in ("lister", "checker"):
            # Get database table metadata for events_to_process table
            metadata = MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables["events_to_process"]

            # Step 1: Mark event as being processed to prevent duplicate processing
            stmt = update(table).where(table.c.event_id == event_num).values(is_being_processed=1)
            with engine.begin() as conn:
                conn.execute(stmt)

            try:
                # Step 2: Attempt to enqueue the processed data for downstream processing
                add_item_to_queue_with_bucket(payload, process, bucket_name)
                logger.info("Enqueued into %s queue", process)

            except Exception as e:
                logger.error("Queue or DB update failed: %s", e)

                # Step 3: Reset processing flag on failure to allow retry
                rollback_stmt = update(table).where(table.c.event_id == event_num).values(is_being_processed=0)
                with engine.begin() as conn:
                    conn.execute(rollback_stmt)

                # Log the queue failure for monitoring
                log_error_to_db(engine, venue_name=venue_name, venue_id=str(venue_id), event_name=evt_name,
                                event_id=str(event_num), event_date=evt_date, event_time=evt_time,
                                error_details=str(e), process_name=process)

        save_eventData_to_db(json.dumps({ "event_data": df.to_dict(orient="records") }))

        return {
            "statusCode": 200,
            "body": payload,
            "headers": {"Content-Type": "application/json"}
        }

    except Exception as e:
        logger.exception("Lambda failed")
        if engine:
            log_error_to_db(engine, venue_name=venue_name, venue_id=str(venue_id), event_name=evt_name,
                            event_id=str(event_num), event_date=evt_date, event_time=evt_time,
                            error_details=str(e), process_name=process)

        # Create error payload from original event data
        body = event.get("parsed", {})  # Get original parsed event data
        payload = {
            "status": "error",  # Mark as error status
            "reason": str(e),  # Include error message as reason
            "error": str(e),  # Include error message in error field
            "event_id": body.get("event_id", ""),  # Get event ID from original data
            "venue_id": body.get("venue_id", 0),  # Get venue ID from original data
            "venue_name": body.get("venue_name", "Walhalla Performing Arts Center"),  # Get venue name with default
            "event_name": body.get("event_name", ""),  # Get event name from original data
            "event_date": evt_date,  # Use parsed event date
            "event_time": evt_time,  # Use parsed event time
            "event_data": []  # Empty data array for failed processing
        }

        # Attempt to enqueue error payload for lister/checker processes
        if body.get("process_name") in ("lister", "checker"):
            try:
                add_item_to_queue_with_bucket(payload, body.get("process_name"), bucket_name)  # Add to processing queue
            except Exception as qe:
                logger.error("Enqueue failed: %s", qe)  # Log queue failure
                if engine:  # Only log to database if engine was successfully created
                    log_error_to_db(engine, venue_name=venue_name, venue_id=str(venue_id), event_name=evt_name,
                                    event_id=str(event_num), event_date=evt_date, event_time=evt_time,
                                    error_details=str(qe), process_name=process)

        return {
            "statusCode": 500,  # Internal server error status
            "body": json.dumps(payload),  # JSON-encoded error payload
            "headers": {"Content-Type": "application/json"}
        }

if __name__ == "__main__":
    event = {
            "parsed": {
                "event_unique_id": "11621868",
                "process_name": "lister",
                "event_id": "5723584",
                "venue_id": "20030",
                "event_name": "WPAC Green Suite",
                "event_datetime": "2025-09-14 15:00:00",
                "event_url": "https://ci.ovationtix.com/36289/performance/11621868"
            } }  
    lambda_handler(event, None)