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
from bradley_playhouse_api import get_list_of_events       # Fetches events from The Bradley Playhouse api
from error_logger import log_error_to_db           # Logs error details to database

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constant format for SkyBox datetime strings
SKY_DT_FMT  = "%Y-%m-%d %H:%M"


def lambda_handler(event, _ctx):
    """
    AWS Lambda entry point for the The Bradley Playhouse event crawler.

    This function orchestrates the complete event synchronization workflow between
    SkyBox (primary ticketing platform) and The Bradley Playhouse APIs.
    It performs intelligent event matching, deduplication, and database storage.

    Process Overview:
    1. Parse incoming Lambda event and extract configuration
    2. Establish database connection and load processing parameters
    3. Fetch events from SkyBox API (authoritative source for venue events)
    4. Fetch events from The Bradley Playhouse API (venue-specific data)
    5. Normalize and preprocess event data from both sources
    6. Match events using fuzzy string matching and exact datetime comparison
    7. Filter out duplicates, cancelled events, and events within skip window
    8. Validate matched events and prepare for database insertion
    9. Deduplicate against existing database records
    10. Insert new events into the processing queue
    """
    logger.info("[lambda_handler] Received event: %s", json.dumps(event))

    # Parse incoming event and extract venue name
    parsed = event.get("parsed", {})
    venue_name = parsed.get("venue_name", "The Bradley Playhouse")
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

    # === PHASE 1: FETCH SKYBOX EVENTS (PRIMARY DATA SOURCE) ===
    # SkyBox serves as the authoritative source for venue events and ticketing data
    logger.info("[lambda_handler] Fetching SkyBox events...")
    try:
        # Call SkyBox API to retrieve events for the specified venue and date range
        skyBox_Response = get_event(
            venue_name,                    # Target venue name for event filtering
            "False",                       # showSoldOut parameter - exclude sold out events
            d_from,                        # Start date for event range (YYYY-MM-DD)
            d_to,                          # End date for event range (YYYY-MM-DD)
            cfg,                           # Configuration object with API credentials
            retries                        # Number of retry attempts for API resilience
        )

        # Validate API response structure and content
        if skyBox_Response is None:
            raise Exception("SkyBox API returned None")

        if not isinstance(skyBox_Response, dict) or "rows" not in skyBox_Response:
            raise Exception(f"SkyBox API returned invalid response format: {skyBox_Response}")

        skyBox_Events = skyBox_Response["rows"]
        if not isinstance(skyBox_Events, list):
            raise Exception(f"SkyBox API rows is not a list: {type(skyBox_Events)}")

        logger.info("[lambda_handler] SkyBox rows fetched: %d", len(skyBox_Events))
    except Exception as e:
        # Handle SkyBox API failures with comprehensive error logging
        err_msg = f"Failed to fetch SkyBox events: {e}"
        logger.error("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
        eng.dispose()  # Clean up database connection
        return {"statusCode": 500, "body": json.dumps("SkyBox fetch failed")}

    # === PHASE 2: PREPROCESS SKYBOX EVENTS ===
    # Normalize SkyBox event data for consistent matching with Walhalla events
    skyBox_Tuples: List[tuple[str, str]] = []  # Will store (datetime_string, cleaned_name, original_event)

    for skyBox_Event in skyBox_Events:
        # Parse and normalize the event datetime from ISO format
        # Remove timezone info and microseconds for consistent formatting
        dt_obj = datetime.fromisoformat(skyBox_Event["date"].rstrip("Z").split(".", 1)[0])

        # Apply business rule: Skip events within the "skip window" (typically next 7 days)
        # This prevents processing events that are too imminent for ticket processing
        if today.date() <= dt_obj.date() <= (today + timedelta(days=daysToSkip)).date():
            logger.info("Skipping event within next %d days: %s on %s", daysToSkip, skyBox_Event["name"], dt_obj)
            continue

        # Format datetime for consistent comparison with Walhalla events
        dt_str = dt_obj.strftime(SKY_DT_FMT)

        # Clean and normalize event name for fuzzy matching:
        # 1. Remove HTML tags using regex
        # 2. Decode HTML entities (e.g., &amp; -> &)
        # 3. Remove special characters that could interfere with matching
        # 4. Convert to lowercase and strip whitespace
        clean = re.sub(r'[<>:&"/\\|?*\'\x00-\x1F]', " ",
                       html.unescape(re.sub(r"<.*?>", "", skyBox_Event["name"]))).lower().strip()

        # Store the processed event data for later matching
        skyBox_Tuples.append((dt_str, clean, skyBox_Event))

    logger.info("[lambda_handler] Normalized SkyBox event names and timestamps.")

    # Fetch The Bradley Playhouse widget events
    logger.info("[lambda_handler] Fetching The Bradley Playhouse widget events...")
    try:
        bradley_Events = get_list_of_events(days_ahead)
        
        if bradley_Events is None:
            raise Exception("The Bradley Playhouse API returned None")
        
        if not isinstance(bradley_Events, list):
            raise Exception(f"The Bradley Playhouse API returned invalid format: {type(bradley_Events)}")
            
        logger.info("[lambda_handler] The Bradley Playhouse events fetched: %d", len(bradley_Events))
    except Exception as e:
        err_msg = f"Failed to fetch The Bradley Playhouse events: {e}"
        logger.error("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
        eng.dispose()
        return {"statusCode": 500, "body": json.dumps("The Bradley Playhouse fetch failed")}

    # Match widget events with SkyBox events
    new_rows: List[Dict[str, any]] = []
    try:
        for bradley_Event in bradley_Events:
            # Validate event structure
            if not isinstance(bradley_Event, dict):
                logger.warning("[lambda_handler] Skipping invalid event format: %s", bradley_Event)
                continue
                
            if not all(key in bradley_Event for key in ['event_date', 'event_time', 'event_name']):
                logger.warning("[lambda_handler] Skipping event missing required fields: %s", bradley_Event.get("event_name", "Unknown"))
                continue
                
            try:
                dt_obj = datetime.strptime(f"{bradley_Event['event_date']} {bradley_Event['event_time']}", "%Y-%m-%d %H:%M:%S")
            except Exception as dt_error:
                logger.warning("[lambda_handler] Skipping invalid datetime for event: %s - %s", bradley_Event.get("event_name"), dt_error)
                continue

            today = datetime.now()
            if today.date() <= dt_obj.date() <= (today + timedelta(days=daysToSkip)).date():
                logger.info("[lambda_handler] Skipping event within next %d days: %s on %s", daysToSkip, bradley_Event["event_name"], dt_obj)
                continue

            matched = None
            for sb in skyBox_Events:
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

                name_evt = bradley_Event["event_name"].lower().strip()
                name_sky = re.sub(r'[<>:&"/\\|?*\'\x00-\x1F]', " ",
                                  html.unescape(re.sub(r"<.*?>", "", sb["name"]))).lower().strip()

                if dt_evt_fmt == dt_sky_fmt and fuzz.partial_ratio(name_evt, name_sky) >= fuzzyNumber:
                    matched = sb
                    break

            if matched is None:
                logger.info("[lambda_handler] Skipping unmatched event: %s (%s)", bradley_Event.get("event_name"), bradley_Event.get("show_id"))
                continue

            logger.info("[lambda_handler] Matched event: %s", bradley_Event["event_name"])

            # Validate matched event structure
            if not all(key in matched for key in ['id', 'name', 'venue']):
                logger.warning("[lambda_handler] Skipping matched event with missing fields: %s", matched)
                continue
                
            if not isinstance(matched.get("venue"), dict) or "id" not in matched["venue"]:
                logger.warning("[lambda_handler] Skipping matched event with invalid venue: %s", matched)
                continue

            new_rows.append({
                "event_id":        str(matched["id"]),
                "event_unique_id": str(bradley_Event.get("event_unique_id", "")),
                "event_name":      matched["name"],
                "event_url":       bradley_Event.get("event_url", ""),
                "event_datetime":  dt_obj,
                "venue_name":      venue_name,
                "venue_id":        str(matched["venue"]["id"]),
                "status":          "active",
                "last_checked":    False,
                "is_listed":       False
            })
    except Exception as e:
        err_msg = f"Error while processing The Bradley Playhouse events: {e}"
        logger.error("[lambda_handler] %s", err_msg)
        log_error_to_db(eng, venue_name=venue_name, error_details=err_msg, process_name="crawler")
        eng.dispose()
        return {"statusCode": 500, "body": json.dumps("Widget event processing failed")}

    if not new_rows:
        logger.info("[lambda_handler] No matched events to insert.")
        eng.dispose()
        return {"statusCode": 200, "body": json.dumps("The Bradley Playhouse crawl finished – no matches")}

    logger.info("[lambda_handler] Total matched events to process: %d", len(new_rows))

    # Add this logging to see what events were matched:
    for i, row in enumerate(new_rows[:5]):  # Log first 5 events
        logger.info("[lambda_handler] Matched event %d: %s on %s", 
                    i+1, row["event_name"], row["event_datetime"])

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
        return {"statusCode": 200, "body": json.dumps("The Bradley Playhouse crawl completed, no new showtimes")}

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

    return {"statusCode": 200, "body": json.dumps("The Bradley Playhouse crawl completed successfully")}

