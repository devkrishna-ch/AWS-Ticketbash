import ast
import json
import os
import logging
import sys
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, update
from app.read_config import read_config
from app.athens_scraper import scrape_event
from app.orchestrator_api import add_item_to_queue_with_bucket
from app.error_logger import log_error_to_db

# from read_config import read_config
# from athens_scraper import scrape_event
# from orchestrator_api import add_item_to_queue_with_bucket
# from error_logger import log_error_to_db


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# # config for production
# config = read_config()
# DB_NAME     = config.get("DB_NAME")
# DB_PORT     = config.get("DB_PORT")
# DB_HOST     = config.get("DB_HOST")
# DB_PASSWORD = config.get("DB_PASSWORD")
# DB_USER     = config.get("DB_USER")
# bucket_name = config.get("BucketName", "")

#Environment variables for database connection for local development
from dotenv import load_dotenv
load_dotenv()
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
bucket_name = os.getenv("BucketName")


def save_eventData_to_db(response_body: str) -> None:
    payload = json.loads(response_body)
    rows = payload.get("event_data", [])
    logger.info("Saving scraped_Data event_data sample: %s", rows[:1])

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

    # Add timestamp processing
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
        logger.info("Saved %s rows to scraper_data", len(df))
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

# ─── Helper to handle scrape/no-data failures ────────────────────────────────
def handle_failure(engine, err, process, venue_name, venue_id, evt_name, evt_date, evt_time, event_num):
    logger.error("Failure handled: %s", err)

    payload = {
        "reason": str(err),
        "status": "error",
        "event_id": event_num,
        "venue_id": venue_id,
        "venue_name": venue_name,
        "event_name": evt_name,
        "event_date": evt_date,
        "event_time": evt_time,
        "event_data": []
    }

    log_error_to_db(engine, venue_name, str(venue_id), evt_name, str(event_num),
                    evt_date, evt_time, str(err), process)

    if process in ("lister", "checker"):
        metadata = MetaData()
        metadata.reflect(bind=engine)
        table = metadata.tables["events_to_process"]       

        with engine.begin() as conn:
                conn.execute(update(table).where(table.c.event_id == event_num).values(error_count = table.c.error_count + 1))

        try:
            add_item_to_queue_with_bucket(payload, process, bucket_name)
            logger.info("Successfully added error payload into %s queue", process)

        except Exception as qe:
            logger.error("Failed to add error payload to queue: %s", qe)

            # rollback status
            # with engine.begin() as conn:
            #     conn.execute(update(table).where(table.c.event_id == event_num).values(is_being_processed=0, in_sqs = 0, error_count = table.c.error_count + 1))
            if engine:
                log_error_to_db(engine, venue_name, str(venue_id), evt_name, str(event_num),
                                evt_date, evt_time, str(qe), process)

    return {"statusCode": 200, "body": payload, "headers": {"Content-Type": "application/json"}}

def lambda_handler(event, context):
    logger.info("lambda_handler invoked with event = %s", event)

    engine = None
    venue_name, evt_name, evt_date, evt_time, process = "", "", "", "", ""
    event_num, venue_id = None, None

    try:
        # ─── Parse event body ──────────────────────────────────────────────
        body = event.get("parsed", {})
        process = body.get("process_name", "")

        event_unique_id = body["event_unique_id"]
        skybox_id = body["event_id"]
        venue_id = int(body.get("venue_id", 0))
        venue_name = body.get("venue_name", "Athens Theatre")
        evt_name = body.get("event_name", "")
        evt_date, evt_time = body["event_datetime"].split(" ")
        event_url = body.get("event_url", "")

        event_num = int(str(skybox_id).split("_")[-1]) if "_" in str(skybox_id) else int(skybox_id)

        # ─── DB connection ─────────────────────────────────────────────────
        engine_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(engine_url)

        # ─── Scraping block (inner try) ────────────────────────────────────
        try:
            scraped_Data = scrape_event(venue_name, event_unique_id)
            try:
                out = json.loads(scraped_Data)
            except json.JSONDecodeError:
                out = ast.literal_eval(scraped_Data)

            # scraper explicit error
            if out.get("status") == "error":
                raise Exception(out.get("message", "Unknown scraper error"))

            data = out.get("event_data", [])
            
            # Log scraper response summary for debugging
            logger.info("Scraper returned %d records. Status: %s", 
                       len(data), out.get("status", "unknown"))

        except Exception as scrape_err:
            # Log the full scraper response on error for debugging
            logger.error("Scrape failed. Raw response: %s", scraped_Data if 'scraped_Data' in locals() else "No response")
            return handle_failure(
                engine, scrape_err, process, venue_name, venue_id,
                evt_name, evt_date, evt_time, event_num
            )

        # ─── No data case ─────────────────────────────────────────────────
        if not data:
            # Extract error context from scraper response
            error_context = (out.get("message") or out.get("details") or 
                           out.get("warning") or "No seat data found")
            
            logger.warning("Empty data returned. Scraper response: %s", out)
            
            return handle_failure(
                engine, error_context, process, venue_name, venue_id,
                evt_name, evt_date, evt_time, event_num
            )

        # ─── Success ──────────────────────────────────────────────────────
        payload = {
            "reason": "success",
            "status": "success",
            "event_id": event_num,
            "venue_id": venue_id,
            "venue_name": venue_name,
            "event_name": evt_name,
            "event_date": evt_date,
            "event_time": evt_time,
            "event_data": data,
        }

        # Normalization
        df = pd.DataFrame(data)
        df.rename(columns={
            "Venue Name": "venue_name", "Event Name": "event_name",
            "Event Date": "event_date", "Event Time": "event_time",
            "Section": "section", "Row": "row", "Seat": "seat_no",
            "Price": "price", "Desc": "description", "UniqueIdentifier": "unique_id",
            "TimeStamp": "timestamp", "Seat Type": "seat_type"
        }, inplace=True)

        payload["event_data"] = df.to_dict(orient="records")

        # ─── Queue & DB Update ─────────────────────────────────────────────
        if process in ("lister", "checker"):
            metadata = MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables["events_to_process"]

            # mark as processing
            with engine.begin() as conn:
                conn.execute(update(table).where(table.c.event_id == event_num).values(is_being_processed=1))

            try:
                add_item_to_queue_with_bucket(payload, process, bucket_name)
                logger.info("Successfully added payload to %s queue", process)

            except Exception as queue_err:
                logger.error("Failed to add payload to queue: %s", queue_err)

                # rollback status
                with engine.begin() as conn:
                    conn.execute(update(table).where(table.c.event_id == event_num).values(is_being_processed=0))
                    # conn.execute(update(table).where(table.c.event_id == event_num).values(is_being_processed=0, in_sqs = 0, error_count = table.c.error_count + 1))

                log_error_to_db(engine, venue_name, str(venue_id), evt_name, str(event_num),
                                evt_date, evt_time, str(queue_err), process)

        # save seat data
        save_eventData_to_db(json.dumps({ "event_data": df.to_dict(orient="records") }))

        return {"statusCode": 200, "body": payload, "headers": {"Content-Type": "application/json"}}

    except Exception as e:
        # ─── Outer fallback ───────────────────────────────────────────────
        logger.exception("Lambda failed")

        if engine:
            log_error_to_db(engine, venue_name, str(venue_id), evt_name, str(event_num),
                            evt_date, evt_time, str(e), process)

        body = event.get("parsed", {})
        payload = {
            "status": "error",
            "reason": str(e),
            "error": str(e),
            "event_id": body.get("event_id", ""),
            "venue_id": body.get("venue_id", 0),
            "venue_name": body.get("venue_name", venue_name),
            "event_name": body.get("event_name", ""),
            "event_date": evt_date,
            "event_time": evt_time,
            "event_data": []
        }

        if process in ("lister", "checker"):
            metadata = MetaData()
            metadata.reflect(bind=engine)
            table = metadata.tables["events_to_process"]            
            try:
                add_item_to_queue_with_bucket(payload, process, bucket_name)
                logger.info("Successfully added error payload into %s queue", process)
            except Exception as qe:
                logger.error("Failed to add error payload to queue: %s", qe)

                # rollback status
                with engine.begin() as conn:
                    conn.execute(update(table).where(table.c.event_id == event_num).values(is_being_processed=0))
                    # conn.execute(update(table).where(table.c.event_id == event_num).values(is_being_processed=0, in_sqs = 0, error_count = table.c.error_count + 1))
                if engine:
                    log_error_to_db(engine, venue_name, str(venue_id), evt_name, str(event_num),
                                    evt_date, evt_time, str(qe), process)

        return {"statusCode": 500, "body": json.dumps(payload), "headers": {"Content-Type": "application/json"}}


# event = {
#   "parsed": {
#     "id": 705,
#     "event_id": 5822400,
#     "event_name": "Every Christmas Story Ever Told",
#     "event_url": "https://cincyshakes.com/book-now?id=1403",
#     "event_datetime": "2025-12-14 14:00:00",
#     "venue_name": "Athens Theatre",
#     "venue_id": 47277,
#     "status": "active",
#     "last_checked": "0000-00-00 00:00:00",
#     "is_listed": 0,
#     "event_unique_id": "163403",
#     "is_being_processed": 0,
#     "scraper_func_name": "cincyshakes_scraper",
#     "process_name": "checker"
#   }
# }

# lambda_handler(event, None)



