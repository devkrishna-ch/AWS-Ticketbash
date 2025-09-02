from __future__ import annotations

import logging
from datetime import datetime, timedelta

from curl_cffi import requests           # lightweight replacement for requests
from dateutil import parser              # robust ISO parsing

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_OVT_URL = "https://ci.ovationtix.com/35617/performance"
CLIENT_ID   = "35617"                                       # Ephrata Theater Theater
CAL_URL     = "https://web.ovationtix.com/trs/api/rest/CalendarProductions"
HEADERS     = {
    "Accept"        : "*/*",
    "Content-Type"  : "application/json",
    "Origin"        : "https://ci.ovationtix.com",
    "Referer"       : "https://ci.ovationtix.com/",
    "User-Agent"    : "Mozilla/5.0",
    "clientId"      : CLIENT_ID,
    "newCIRequest"  : "true",
}


def _within_range(show_dt: str, start: datetime, end: datetime) -> bool:
    try:
        dt = parser.isoparse(show_dt)
    except Exception:
        return False
    return start <= dt <= end


def get_list_of_events(days_forward: int = 730) -> list[dict]:
    now     = datetime.utcnow()
    cut_off = now + timedelta(days=days_forward)

    resp = requests.get(CAL_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    perf: list[dict] = []
    for day in payload:
        for prod in day.get("productions", []):
            pid        = str(prod.get("productionId", ""))
            title      = prod.get("name", "").strip()
            sel_method = (prod.get("seatSelectionMethod") or "").lower()

            for show in prod.get("showtimes", []):
                start_iso = show.get("performanceStartTime", "")
                if not _within_range(start_iso, now, cut_off):
                    continue

                # Filter out unavailable, cancelled, or sold out events
                is_available = bool(show.get("performanceAvailable"))
                is_cancelled = bool(show.get("isCancelled"))
                is_soldout = bool(show.get("isSoldOut"))
                
                if not is_available or is_cancelled or is_soldout:
                    continue

                dt = parser.isoparse(start_iso)
                perf.append({
                    "event_id"              : pid,
                    "event_unique_id"       : str(show.get("performanceId")),
                    "event_name"            : title,
                    "event_date"            : dt.date().isoformat(),
                    "event_time"            : dt.time().strftime("%H:%M:%S"),
                    "seat_selection_method" : sel_method,
                    "event_url"             : f"{BASE_OVT_URL}/{show.get('performanceId')}"
                })

    logger.info("Ephrata Performing Arts Center API → %d performances in next %d days",
                len(perf), days_forward)
    return perf
