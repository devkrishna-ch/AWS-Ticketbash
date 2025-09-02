from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CLIENT = "athenstheatre"
BASE   = f"https://app.spektrix-link.com/clients/athenstheatre"

EVENTS_VIEW_URL  = f"{BASE}/eventsView.json"
EVENT_DETAIL_URL = f"{BASE}/events/{{numeric_id}}.json"
CHOOSE_SEATS_URL = "https://booking.athensdeland.com/ChooseSeats/{digits}"

UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

def _json(url: str, timeout: int = 10):
    try:
        res = requests.get(url, headers=UA_HEADERS, timeout=timeout)
        res.raise_for_status()
        return res.json()
    except Exception as exc:
        logger.warning("GET %s failed: %s", url, exc)
        return None


def _numeric_prefix(full_id: str) -> str | None:
    """Return leading digits of a Spektrix ID (e.g. '22601' ← '22601ABC…')."""
    m = re.match(r"(\d+)", full_id or "")
    return m.group(1) if m else None

def _get_event_instances() -> List[Dict]:
    """
    Build one dict per *instance*:

        {
            event_name,            # Athens Show
            event_url,             # ChooseSeats/28406
            startDate              # ISO timestamp
        }
    """
    root = _json(EVENTS_VIEW_URL, timeout=15)
    if not root:
        logger.error("Failed to fetch eventsView.json")
        return []

    out: List[Dict] = []

    for ev in root:
        full_event_id: str = ev.get("id", "")
        numeric_event_id   = _numeric_prefix(full_event_id)
        event_name         = ev.get("name", "Unknown Event")
        print(f"\nEvent ID: {numeric_event_id}")
        print(f"Event Name: {event_name}")
        if not numeric_event_id:
            continue

        detail = _json(EVENT_DETAIL_URL.format(numeric_id=numeric_event_id))
        if not detail:
            continue

        # Prefer 'instances', but some payloads expose 'instancesWebMode'
        instances = detail.get("instances") or detail.get("instancesWebMode") or []
        if not instances:
            # final fallback to lastAvailableInstanceId + firstInstanceDateTime
            last_id = ev.get("lastAvailableInstanceId")
            first_ts = ev.get("firstInstanceDateTime")
            if last_id and first_ts:
                instances = [{"id": last_id, "start": first_ts}]

        for inst in instances:
            inst_id_full = inst.get("id", "")
            inst_digits  = _numeric_prefix(inst_id_full)
            iso_ts       = inst.get("start") or inst.get("instanceDateTime")
            print("Instance ID:", inst_digits)
            print("Date/Time: ", iso_ts)
            if not inst_digits or not iso_ts:
                continue

            out.append(
                {
                    "event_id":  numeric_event_id,                 # <── NEW
                    "event_name": event_name,
                    "event_url":  CHOOSE_SEATS_URL.format(digits=inst_digits),
                    "startDate":  iso_ts,
                    "inst_digits": inst_digits
                }
            ) 

    logger.info("Extracted %d Athens Theatre instances", len(out))
    print(out)
    return out

# ──────────────────────────────────────────────────────────────
# PUBLIC EXPORT
# ──────────────────────────────────────────────────────────────
def get_event_instances() -> List[Dict]:
    """
    Public helper so other modules (e.g. the Lambda) can fetch the
    exact list produced by _get_event_instances() without relying
    on the private underscore‑prefixed name.
    """
    return _get_event_instances()


def get_list_of_events(date_from: str, date_to: str, max_retries: int = 3) -> List[Dict]:
    """
    Return list of dicts matching the structure expected by your Lambda.
    """
    logger.info("Collecting Athens Theatre events — %s → %s", date_from, date_to)

    raw = _get_event_instances()
    events: List[Dict] = []

    for ev in raw:
        try:
            dt = datetime.fromisoformat(ev["startDate"])
        except Exception:
            dt = datetime.now(timezone.utc)
        events.append(
            {
                "event_id":   ev["event_url"],   # good-enough unique key
                "event_url":  ev["event_url"],
                "event_name": ev["event_name"],
                "show_id":    ev["event_url"],
                "event_date": dt.strftime("%Y-%m-%d"),
                "event_time": dt.strftime("%H:%M:%S"),
                "event_unique_id": (
                            f"{ev['inst_digits']}|{ev['event_name']}|"
                            f"{dt.strftime('%Y-%m-%d')}|{dt.strftime('%H:%M:%S')}"
                        ),            }
        )

    logger.info("Prepared %d instances in widget format", len(events))
    return events

