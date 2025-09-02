# chanhassen_api.py
"""
Minimal helper for the Chanhassen Dinner Theatres WordPress audience-view
endpoint: https://chanhassendt.com/wp-json/wpbm-audience-view/v1/shows
"""

import logging, time, random
from datetime import datetime
from dateutil import parser
from curl_cffi import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE   = "https://chanhassendt.com/wp-json/wpbm-audience-view/v1/shows"
UA_STR = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
          " AppleWebKit/537.36 (KHTML, like Gecko)"
          " Chrome/135.0.0.0 Safari/537.36")


def _page(page: int = 1):
    r = requests.get(BASE,headers={"user-agent": UA_STR},params={"page": page, "limit": 100},timeout=30)
    return r.status_code, (r.json() if r.status_code == 200 else None)


def get_events(date_from,date_to,max_retries = 3):
    """
    Returns events with at least one PUBLIC upcoming performance
    in [date_from, date_to] (YYYY-MM-DD).
    """
    page = 1
    out  = []
    while True:
        for attempt in range(max_retries):
            code, data = _page(page)
            if code == 200:
                break
            if code == 429:
                delay = random.randint(4, 8)
                logger.warning("429 – retry page %s after %s s", page, delay)
                time.sleep(delay)
            else:
                logger.warning("Widget page %s returned %s – retry", page, code)
                time.sleep(3)
        else:
            raise RuntimeError(f"Chan widget page {page} failed repeatedly")

        items = data.get("items", [])
        if not items:
            break

        for it in items:
            if not it.get("has_upcoming_performances"):
                continue
            name_clean = it["post_title"]
            ticket_url = it["ticket_link"]
            article_id = (
                ticket_url.split("article_id=")[-1]
                if "article_id=" in ticket_url else ""
            )

            for perf in it.get("upcoming_performances", []):
                if not isinstance(perf, dict):
                    logger.warning("Unexpected perf type: %s", type(perf))
                    continue

                if perf.get("access", "").lower() != "public":
                    continue
                if perf.get("availability_status", "").lower() in ("s", "u"):
                    continue

                dt = parser.parse(perf["start_date"])
                if not (date_from <= dt.date().isoformat() <= date_to):
                    continue

                out.append({
                    "event_id":   article_id,
                    "event_name": name_clean,
                    "show_id":    perf["id"],
                    "event_date": dt.date().isoformat(),
                    "event_time": dt.time().strftime("%H:%M:%S"),
                    "event_url":  ticket_url,
                })

        if page >= data.get("page_count", 1):
            break
        page += 1

    return out


def get_shows_for_event(performance_id: str) -> list[dict]:
    shows = []
    for ev in get_list_of_events("1900-01-01", "2100-12-31"):
        if ev["show_id"] == performance_id:
            shows.append({"eventDate": ev["event_date"], "eventTime": ev["event_time"]})
    return shows
