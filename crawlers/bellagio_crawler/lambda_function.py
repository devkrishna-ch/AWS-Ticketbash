# Standard library imports
import json, logging, html, re
from datetime import datetime, timedelta
from typing import List, Dict

# Third-party library imports
import pandas as pd
from sqlalchemy import create_engine, types
from sqlalchemy.exc import IntegrityError
from thefuzz import fuzz  # For fuzzy string matching

# Custom module imports
from read_config import read_config                # Reads config from external source
from skybox_api import get_event                   # Fetches events from SkyBox API
from bellagio_api import get_list_of_events      # Fetches events from Bellagio widget
from error_logger import log_error_to_db           # Logs error details to database

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constant format for SkyBox datetime strings
SKY_DT_FMT  = "%Y-%m-%d %H:%M"


def lambda_handler(event, _ctx):
    """
    AWS Lambda entry point for crawling events from the SkyBox and Bellagio APIs.
    Compares and deduplicates events, then stores new ones in the database.
    """
    logger.info("[lambda_handler] Received event: %s", json.dumps(event))

    # Parse incoming event and extract venue name
    parsed = event.get("parsed", {})
    venue_name = parsed.get("venue_name", "Bellagio")
    logger.info("[lambda_handler] Venue name set to: %s", venue_name)

    # Load configuration
    cfg = read_config()
    logger.info("[lambda_handler] Config read successfully.")

    # Create DB engine
    eng = create_engine(
        f"mysql+pymysql://{cfg['DB_USER']}:{cfg['DB_PASSWORD']}@{cfg['DB_HOST']}:{cfg['DB_PORT']}/{cfg['DB_NAME']}"
    )

    # Read config values for filtering/matching
    days_ahead   = int(cfg["Days"])
    daysToSkip   = int(cfg["DaysToSkip"])
    fuzzyNumber  = int(cfg["FuzzyNumber"])
    canceledList = cfg["CancelledEvents"]
    logger.info("[lambda_handler] Days ahead: %d | Days to skip: %d | Fuzzy number: %d", days_ahead, daysToSkip, fuzzyNumber)

    # Calculate date range for event fetching
    today   = datetime.now()
    look_to = today + timedelta(days=days_ahead)
    d_from  = today.strftime("%Y-%m-%d")
    d_to    = look_to.strftime("%Y-%m-%d")
    retries = 3
    logger.info("[lambda_handler] Date range: %s to %s", d_from, d_to)

    # Fetch events from SkyBox API
    logger.info("[lambda_handler] Fetching SkyBox events...")
    try:
        sky_response = get_event(
            venue_name,
            "False",  # showSoldOut parameter
            d_from,
            d_to,
            cfg,
            retries
        )
        
        if sky_response is None:
            raise Exception("SkyBox API returned None")
        
        if not isinstance(sky_response, dict) or "rows" not in sky_response:
            raise Exception(f"SkyBox API returned invalid response format: {sky_response}")
            
        sky_rows = sky_response["rows"]
        if not isinstance(sky_rows, list):
            raise Exception(f"SkyBox API rows is not a list: {type(sky_rows)}")
            
        logger.info("[lambda_handler] SkyBox rows fetched: %d", len(sky_rows))
    except Exception as e:
        err_msg = f"Failed to fetch SkyBox events: {e}"
        logger.error("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
        eng.dispose()
        return {"statusCode": 500, "body": json.dumps("SkyBox fetch failed")}

    # Preprocess SkyBox events (normalize, filter)
    sky_tuples: List[tuple[str, str]] = []
    for r in sky_rows:
        dt_obj = datetime.fromisoformat(r["date"].rstrip("Z").split(".", 1)[0])
        if today.date() <= dt_obj.date() <= (today + timedelta(days=daysToSkip)).date():
            logger.info("Skipping event within next 7 days: %s on %s", r["name"], dt_obj)
            continue
        dt_str = dt_obj.strftime(SKY_DT_FMT)
        clean = re.sub(r'[<>:&"/\\|?*\'\x00-\x1F]', " ",
                       html.unescape(re.sub(r"<.*?>", "", r["name"]))).lower().strip()
        sky_tuples.append((dt_str, clean, r))
    logger.info("[lambda_handler] Normalized SkyBox event names and timestamps.")

    # Fetch Bellagio widget events
    logger.info("[lambda_handler] Fetching Bellagio widget events...")
    try:
        bellagio_events = get_list_of_events(d_from, d_to)
        
        if bellagio_events is None:
            raise Exception("Bellagio API returned None")
        
        if not isinstance(bellagio_events, list):
            raise Exception(f"Bellagio API returned invalid format: {type(bellagio_events)}")
            
        logger.info("[lambda_handler] Bellagio events fetched: %d", len(bellagio_events))
    except Exception as e:
        err_msg = f"Failed to fetch Bellagio events: {e}"
        logger.error("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
        eng.dispose()
        return {"statusCode": 500, "body": json.dumps("Bellagio fetch failed")}

    # Match widget events with SkyBox events
    new_rows: List[Dict[str, any]] = []
    try:
        for ev in bellagio_events:
            # Validate event structure
            if not isinstance(ev, dict):
                logger.warning("[lambda_handler] Skipping invalid event format: %s", ev)
                continue
                
            if not all(key in ev for key in ['event_date', 'event_time', 'event_name']):
                logger.warning("[lambda_handler] Skipping event missing required fields: %s", ev.get("event_name", "Unknown"))
                continue
                
            try:
                dt_obj = datetime.strptime(f"{ev['event_date']} {ev['event_time']}", "%Y-%m-%d %H:%M:%S")
            except Exception as dt_error:
                logger.warning("[lambda_handler] Skipping invalid datetime for event: %s - %s", ev.get("event_name"), dt_error)
                continue

            today = datetime.now()
            if today.date() <= dt_obj.date() <= (today + timedelta(days=daysToSkip)).date():
                logger.info("[lambda_handler] Skipping event within next %d days: %s on %s", daysToSkip, ev["event_name"], dt_obj)
                continue

            matched = None
            for sb in sky_rows:
                try:
                    if not isinstance(sb, dict) or "date" not in sb:
                        continue
                        
                    dt_sky = datetime.fromisoformat(sb["date"].split(".")[0].rstrip("Z"))
                except Exception:
                    continue

                skybox_event_name = sb.get("name", "").lower()
                if any(word in skybox_event_name for word in canceledList):
                    continue

                dt_evt_fmt = dt_obj.strftime("%m/%d/%Y %I:%M:%S %p")
                dt_sky_fmt = dt_sky.strftime("%m/%d/%Y %I:%M:%S %p")

                name_evt = ev["event_name"].lower().strip()
                name_sky = re.sub(r'[<>:&"/\\|?*\'\x00-\x1F]', " ",
                                  html.unescape(re.sub(r"<.*?>", "", sb["name"]))).lower().strip()

                if dt_evt_fmt == dt_sky_fmt and fuzz.partial_ratio(name_evt, name_sky) >= fuzzyNumber:
                    matched = sb
                    break

            if matched is None:
                logger.info("[lambda_handler] Skipping unmatched event: %s (%s)", ev.get("event_name"), ev.get("show_id"))
                continue

            logger.info("[lambda_handler] Matched event: %s", ev["event_name"])

            # Validate matched event structure
            if not all(key in matched for key in ['id', 'name', 'venue']):
                logger.warning("[lambda_handler] Skipping matched event with missing fields: %s", matched)
                continue
                
            if not isinstance(matched.get("venue"), dict) or "id" not in matched["venue"]:
                logger.warning("[lambda_handler] Skipping matched event with invalid venue: %s", matched)
                continue

            new_rows.append({
                "event_id":        str(matched["id"]),
                "event_unique_id": str(ev.get("event_unique_id", "")),
                "event_name":      matched["name"],
                "event_url":       ev.get("event_url", ""),
                "event_datetime":  dt_obj,
                "venue_name":      venue_name,
                "venue_id":        str(matched["venue"]["id"]),
                "status":          "active",
                "last_checked":    False,
                "is_listed":       False,
                "create_at" :       datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    except Exception as e:
        err_msg = f"Error while processing Bellagio events: {e}"
        logger.error("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
        eng.dispose()
        return {"statusCode": 500, "body": json.dumps("Widget event processing failed")}

    if not new_rows:
        logger.info("[lambda_handler] No matched events to insert.")
        eng.dispose()
        return {"statusCode": 200, "body": json.dumps("Bellagio crawl finished – no matches")}

    logger.info("[lambda_handler] Total matched events to process: %d", len(new_rows))

    # Create dataframe for insertion
    df = pd.DataFrame(new_rows)

    # Fetch existing rows to deduplicate
    try:
        existing = pd.read_sql("SELECT event_id, event_unique_id FROM events_to_process", eng)
        logger.info("[lambda_handler] Existing rows fetched: %d", len(existing))
    except Exception as exc:
        err_msg = f"Could not fetch existing rows: {exc}"
        logger.warning("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
        existing = pd.DataFrame(columns=["event_id", "event_unique_id"])

    # Normalize datatypes
    for col in ("event_id", "event_unique_id"):
        df[col] = df[col].astype(str)
        existing[col] = existing[col].astype(str)

    # Filter out duplicates
    new_df = df[~df["event_unique_id"].isin(existing["event_unique_id"]) &
                ~df["event_id"].isin(existing["event_id"])]
    if new_df.empty:
        logger.info("[lambda_handler] No new rows to insert after deduplication.")
        eng.dispose()
        return {"statusCode": 200, "body": json.dumps("Bellagio crawl completed, no new showtimes")}

    new_df = new_df.drop_duplicates(subset=["event_id"])

    # Insert new rows into DB
    try:
        new_df.to_sql(
            "events_to_process",
            eng,
            if_exists="append",
            index=False,
            dtype={
                "event_id":        types.String(100),
                "event_unique_id": types.String(300),
                "event_name":      types.String(255),
                "event_url":       types.String(512),
                "event_datetime":  types.DateTime(),
                "venue_name":      types.String(255),
                "venue_id":        types.String(100),
                "status":          types.String(50),
                "last_checked":    types.Boolean(),
                "is_listed":       types.Boolean(),
                "create_at":       types.DateTime(),
            }
        )
        logger.info("[lambda_handler] Inserted %d new events into the database.", len(new_df))
    except IntegrityError as dup:
        logger.warning("[lambda_handler] IntegrityError – duplicates skipped: %s", dup.orig.args[1])
    except Exception as ex:
        err_msg = f"DB insertion failed: {ex}"
        logger.error("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
    finally:
        eng.dispose()
        logger.info("[lambda_handler] Database connection closed.")

    return {"statusCode": 200, "body": json.dumps("Bellagio crawl completed successfully")}
