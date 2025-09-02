import json
import pandas
import logging
import pandas as pd
import re
from sqlalchemy import create_engine, types
import html
from read_config import read_config
from skybox_api import get_event
from kennedy_center_api import get_list_of_events,check_onsale_date
from curl_cffi import requests
from datetime import datetime, timedelta
from thefuzz import fuzz  



logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        config = read_config()
        venue_name = event.get('parsed', {}).get('venue_name', '')
        if venue_name == '':
            # Set default venue name instead of failing
            venue_name = 'Kennedy Center'
            logger.warning('Venue name is missing, using default: Kennedy Center')

        venue_filter = venue_name  
        retry_count = 3                                   
        exclude_active_inventory = "False"
        today = datetime.now()
        future_date = today + timedelta(days=int(config.get('Days')))
        event_date_from = today.strftime("%Y-%m-%d")
        event_date_to = future_date.strftime("%Y-%m-%d") 
        output_format = "%m/%d/%Y %H:%M:%S %p"
        fuzzyNumber  = int(config["FuzzyNumber"])
        daysToSkip   = int(config["DaysToSkip"])

        logging.info(f"Crawling process started for venue: {venue_name}")
        
        # Initialize DataFrame
        events_df = pd.DataFrame(columns=[
            'event_name', 'event_url', 'event_datetime', 'venue_name', 
            'venue_id', 'status', 'last_checked', 'is_listed', 'event_unique_id'
        ])

        # Step 1: Get SkyBox events (like Helena)
        logger.info("Fetching SkyBox events...")
        try:
            skybox_events = get_event(venue_name, exclude_active_inventory, event_date_from, event_date_to, config, retry_count)
            if not skybox_events or 'rows' not in skybox_events:
                raise Exception("No events returned from Skybox API")
            sky_rows = skybox_events['rows']
            logger.info(f"SkyBox rows fetched: {len(sky_rows)}")
        except Exception as e:
            logger.error(f"Failed to fetch SkyBox events: {e}")
            return {"statusCode": 500, "body": json.dumps("SkyBox fetch failed")}

        # Step 2: Create SkyBox tuples with 7-day buffer (like Helena)
        sky_tuples = []
        for r in sky_rows:
            try:
                dt_obj = datetime.fromisoformat(r["date"].rstrip("Z").split(".", 1)[0])
            except Exception:
                continue
                
            # Apply business rule: Skip events within the "skip window" (typically next 7 days)
            # This prevents processing events that are too imminent for ticket processing
            if today.date() <= dt_obj.date() <= (today + timedelta(days=daysToSkip)).date():
                logger.info("Skipping Skybox event within next %d days: %s on %s",daysToSkip, r['name'], dt_obj)
                continue
                
            # Skip cancelled events
            event_name_lower = r.get('name', '').lower()
            if any(word in event_name_lower for word in ['cancelled', 'canceled', 'postponed']):
                logger.info(f"Skipping cancelled/postponed Skybox event: {r['name']}")
                continue
                
            dt_str = dt_obj.strftime(output_format)
            clean = re.sub(r'[<>:&"/\\|?*\'\x00-\x1F]', " ",
                          html.unescape(re.sub(r"<.*?>", "", r["name"]))).lower().strip()
            sky_tuples.append((dt_str, clean, r))

        logger.info(f"SkyBox events after filtering: {len(sky_tuples)}")

        # Step 3: Get Kennedy Center events
        events_api_url = config.get('Kennedy_Center_EventAPI_URL')
        available_events = get_list_of_events(events_api_url, start_date=event_date_from, end_date=event_date_to)
        logger.info(f"Kennedy Center events fetched: {len(available_events)}")

        # Step 4: Process Kennedy Center events (like Helena)
        new_rows = []
        try:
            for ev in available_events:
                # Clean event name early for consistent logging
                raw_event_name = ev.get("name", "")
                clean_event_name = re.sub(r'[<>:&"/\\|?*\'\x00-\x1F]', " ", 
                                        html.unescape(re.sub(r"<.*?>", "", raw_event_name))).strip()
                
                # Skip unavailable events
                if ev.get("cancelled") or ev.get("soldOut") or not ev.get("onSale"):
                    public_on_sale_date = ev.get("publicOnSaleDate", "")
                    if public_on_sale_date:
                        on_sale_within_5_days = check_onsale_date(public_on_sale_date)
                        if not on_sale_within_5_days:
                            logger.info(f"Skipping event {clean_event_name} - not on sale within next 5 days")
                            continue
                    else:
                        logger.info(f"Skipping unavailable event: {clean_event_name}")
                        continue

                # Parse datetime
                try:
                    event_date_string = ev.get("eventDateString", "").strip()
                    if not event_date_string:
                        logger.warning(f"Skipping event {clean_event_name} - missing eventDateString")
                        continue
                    dt_obj = datetime.strptime(event_date_string, "%m/%d/%Y %I:%M:%S %p")
                except Exception as e:
                    logger.error(f"Failed to parse datetime for event {clean_event_name}: {e}")
                    continue

                # Skip events within 7 days
                if today.date() <= dt_obj.date() <= (today + timedelta(days=daysToSkip)).date():
                    logger.info("Skipping Kennedy Center event within next %d days: %s on %s",daysToSkip, clean_event_name, dt_obj.date())
                    continue

                # Use cleaned name for processing
                event_name = clean_event_name
                venue_name_full = f'Kennedy Center {ev.get("location")}'
                event_id = ev.get("id", "")
                ticket_url = ev.get("buyTicketCtaUrl", "")

                # Check required fields
                if not event_id:
                    logger.error(f"No event id found for Event {event_name}")
                    continue

                # Check venue filter
                if venue_name_full != venue_filter:
                    logger.info(f"Skipping event {event_name} - venue {venue_name_full} doesn't match filter {venue_filter}")
                    continue

                # Match with SkyBox events (like Helena)
                dt_key = dt_obj.strftime(output_format)
                ev_name_clean = event_name.lower().strip()
                date_str = dt_obj.strftime("%Y-%m-%d")   # just date
                time_str = dt_obj.strftime("%H:%M:%S") 

                matched = None
                for sky_dt_str, sky_name, sky_row in sky_tuples:
                    if sky_dt_str == dt_key and fuzz.partial_ratio(ev_name_clean, sky_name) >= fuzzyNumber:
                        matched = sky_row
                        logger.info(f"Matched event: {event_name} with Skybox: {sky_row.get('name', '')}")
                        break

                if matched is None:
                    logger.info(f"Skipping unmatched event: {event_name} on {dt_key}")
                    continue

                # Add to processing list
                new_rows.append({
                    "event_id": matched["id"],
                    "event_unique_id": f"{venue_name_full}|{matched['name']}|{date_str}|{time_str}|{event_id}",
                    "event_name": matched["name"],
                    "event_url": ticket_url,
                    "event_datetime": dt_obj,
                    "venue_name": venue_name_full,
                    "venue_id": matched["venue"]["id"],
                    "status": "active",
                    "last_checked": False,
                    "is_listed": False
                })

        except Exception as e:
            logger.error(f"Error processing Kennedy Center events: {e}")
            return {"statusCode": 500, "body": json.dumps("Event processing failed")}

        # Step 5: Write to database (like Helena)
        if new_rows:
            events_df = pd.DataFrame(new_rows)
            logger.info(f"Found {len(events_df)} new events to process")
            
            try:
                engine = create_engine(
                    f"mysql+pymysql://{config['DB_USER']}:{config['DB_PASSWORD']}@"
                    f"{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}",
                    pool_pre_ping=True
                )
                
                # Check for duplicates
                existing_ids = pd.read_sql(
                    "SELECT DISTINCT event_unique_id FROM events_to_process",
                    engine
                )['event_unique_id'].tolist()
                
                new_events = events_df[~events_df['event_unique_id'].isin(existing_ids)]
                
                if len(new_events) < len(events_df):
                    logger.info(f"Filtered out {len(events_df) - len(new_events)} duplicates")
                
                if not new_events.empty:
                    new_events.to_sql(
                        'events_to_process',
                        engine,
                        if_exists='append',
                        index=False,
                        dtype={
                            'event_id': types.String(length=50),
                            'event_name': types.String(length=255),
                            'event_url': types.String(length=512),
                            'event_datetime': types.DateTime(),
                            'venue_name': types.String(length=255),
                            'venue_id': types.String(length=50),
                            'status': types.String(length=50),
                            'last_checked': types.DateTime(),
                            'is_listed': types.Boolean(),
                            'event_unique_id': types.String(length=512)
                        }
                    )
                    logger.info(f"Successfully wrote {len(new_events)} new events to database")
                else:
                    logger.info("No new events to write after duplicate filtering")
                    
            except Exception as e:
                logger.error(f"Database write failed: {str(e)}")
                raise
            finally:
                if 'engine' in locals():
                    engine.dispose()
        else:
            logger.info("No new events to process")

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        raise

    return {
        'statusCode': 200,
        'body': json.dumps('Processing completed successfully')
    }
if __name__ == "__main__":
    # Simulate AWS Lambda event and context
    event = {}
    context = {}
    
    # Call the lambda_handler function
    response = lambda_handler(event, context)
    
    # Print the response
    print(response)
