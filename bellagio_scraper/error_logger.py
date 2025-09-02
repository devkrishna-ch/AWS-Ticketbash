import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def log_error_to_db(engine, venue_name=None, venue_id=None, event_name=None, 
                   event_id=None, event_date=None, event_time=None, 
                   error_details=None, process_name='scraper'):
    """
    Standard error logging function for database errors.
    
    Args:
        engine: SQLAlchemy engine instance
        venue_name: Name of the venue
        venue_id: ID of the venue
        event_name: Name of the event
        event_id: ID of the event
        event_date: Date of the event
        event_time: Time of the event
        error_details: Detailed error message
        process_name: Name of the process that encountered the error
    """
    try:
        error_data = {
            "venue_name": venue_name,
            "venue_id": venue_id,
            "event_name": event_name or 'test',
            "event_id": event_id,
            "event_date": event_date,
            "event_time": event_time,
            "error_details": error_details,
            "timestamp": datetime.now(),
            "process_name": process_name
        }

        df = pd.DataFrame([error_data])
        df.to_sql("errors", engine, if_exists="append", index=False)

        logger.info(f"Error logged to database: {error_details[:100]}...")

    except Exception as e:
        logger.error(f"Failed to log error to database: {e}")