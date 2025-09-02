from __future__ import annotations

import html
import json
import logging
import os
import re
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, types
from sqlalchemy.exc import IntegrityError
from thefuzz import fuzz

from app.read_config   import read_config
from app.skybox_api    import get_event
from app.athens_api    import get_list_of_events, get_event_instances  # ← new import

# ──────────────────────────────────────────────────────────────
# ENVIRONMENT
# ──────────────────────────────────────────────────────────────

config      = read_config()
DB_NAME     = config.get("DB_NAME")
DB_PORT     = config.get("DB_PORT")
DB_HOST     = config.get("DB_HOST")
DB_PASSWORD = config.get("DB_PASSWORD")
DB_USER     = config.get("DB_USER")
bucket_name = config.get("BucketName", "")

LOOKAHEAD_DAYS = int(config.get("Days", 730))        # default: 2 years
SKY_DT_FMT      = "%Y-%m-%d %H:%M"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ──────────────────────────────────────────────────────────────
# HANDLER
# ──────────────────────────────────────────────────────────────
def lambda_handler(event: dict, context) -> dict:
    cfg = read_config()

    venue_name = event.get("parsed", {}).get("venue_name", "Athens Theatre")
    if not venue_name:
        return _resp(400, "venue_name missing")

    logger.info("Athens crawl started for %s", venue_name)

    # ------------------------------------------------------------------
    # DATE WINDOW
    # ------------------------------------------------------------------
    today   = datetime.now()
    look_to = today + timedelta(days=LOOKAHEAD_DAYS)
    d_from  = today.strftime("%Y-%m-%d")
    d_to    = look_to.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # OPEN DB CONNECTION ONCE
    # ------------------------------------------------------------------
    eng = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # ------------------------------------------------------------------
    # 2️⃣  SKYBOX SOURCE  (still used for venue_id, pricing, etc.)
    # ------------------------------------------------------------------
    sky_rows = get_event(
        venue_name,
        "False",                # showInactive
        d_from,
        d_to,
        cfg,
        3,
    )["rows"]
    logger.info("SkyBox rows fetched: %d", len(sky_rows))

    sky_index: list[tuple[str, str, dict]] = []
    for r in sky_rows:
        dt_sky = datetime.fromisoformat(r["date"].rstrip("Z").split(".", 1)[0])
        name_sky = re.sub(
            r'[<>:&"/\\|?*\'\x00-\x1F]',
            " ",
            html.unescape(re.sub(r"<.*?>", "", r["name"]))
        ).lower().strip()
        sky_index.append((dt_sky.strftime(SKY_DT_FMT), name_sky, r))

    # ------------------------------------------------------------------
    # 3️⃣  WIDGET SOURCE  (Spektrix instances, widget format)
    # ------------------------------------------------------------------
    widget_events = get_list_of_events(d_from, d_to, 3)
    logger.info("Widget performances: %d", len(widget_events))

    # ------------------------------------------------------------------
    # 4️⃣  MATCH + PREPARE ROWS
    # ------------------------------------------------------------------
    rows_to_insert: list[dict] = []

    for ev in widget_events:
        ev_dt = datetime.strptime(
            f"{ev['event_date']} {ev['event_time']}", "%Y-%m-%d %H:%M:%S"
        )

        # skip events in the next 7 days
        if today.date() <= ev_dt.date() <= (today + timedelta(days=7)).date():
            logger.info("Skipping near‑term event %s on %s", ev["event_name"], ev_dt)
            continue

        # fuzzy‑match against SkyBox (to grab venue_id, etc.)
        # matched: dict | None = None
        # for dt_sky_str, name_sky, rec in sky_index:
        #     if fuzz.partial_ratio(ev["event_name"].lower(), name_sky) >= 50:
        #         matched = rec
        #         break

        matched: dict | None = None
        best_delta = 999999
        for dt_sky_str, name_sky, rec in sky_index:
            # title must be similar
            if fuzz.partial_ratio(ev["event_name"].lower(), name_sky) < 50:
                continue

            # choose the SkyBox row whose datetime is closest (same day typically)
            dt_sky = datetime.strptime(dt_sky_str, SKY_DT_FMT)
            delta  = abs((dt_sky - ev_dt).total_seconds())
            if delta < best_delta:
                best_delta = delta
                matched    = rec

        # require the datetime to be reasonably close (e.g. 12 h)
        if matched is None or best_delta > 12*3600:
            logger.info("No SkyBox match for %s – skipping", ev["event_name"])
            continue

        # --------------------------------------------------------------
        # UNIQUE KEYS
        # --------------------------------------------------------------
        m = re.search(r"(?:EventInstanceId=|/ChooseSeats/)(\d+)", ev["event_url"])
        inst_part = m.group(1) if m else f"X{abs(hash(ev['event_url'])) & 0xFFFFF:06x}"
        ev["event_url"] = f"https://booking.athensdeland.com/ChooseSeats/{inst_part}"

        # event_id  = f"{matched['id']}_{inst_part}"
        event_id = str(matched["id"])
        unique_id = (
            f"{inst_part}|{matched['name']}|"
            f"{ev_dt.strftime('%B %-d, %Y')}|{ev_dt.strftime('%-I:%M %p')}|reg"
        )

        # --------------------------------------------------------------
        # BUILD ROW – **event_datetime now uses ev_dt**
        # --------------------------------------------------------------
        rows_to_insert.append(
            {
                "event_id":        event_id,
                "event_unique_id": ev["event_unique_id"],
                "event_name":      ev["event_name"],
                "event_url":       ev["event_url"],
                "event_datetime":  ev_dt,
                "venue_name":      venue_name,
                "venue_id":        matched["venue"]["id"],
                "status":          "active",
                "last_checked":    False,
                "is_listed":       False,
            }
        )

    # ------------------------------------------------------------------
    # 5️⃣  INSERT INTO events_to_process  (unchanged logic)
    # ------------------------------------------------------------------
    if not rows_to_insert:
        logger.info("Nothing new to insert into events_to_process")
        eng.dispose()
        return _resp(200, "Athens crawl finished")

    df = pd.DataFrame(rows_to_insert)

    try:
        existing = pd.read_sql("SELECT event_unique_id FROM events_to_process", eng)
    except Exception as e:
        logger.warning("Could not read existing – assuming empty: %s", e)
        existing = pd.DataFrame(columns=["event_unique_id"])

    df_new = df[~df["event_unique_id"].isin(existing["event_unique_id"])]

    if df_new.empty:
        logger.info("No new rows after de‑dup")
        eng.dispose()
        return _resp(200, "Athens crawl finished")

    try:
        df_new.to_sql(
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
            },
        )
        logger.info("Inserted %d Athens rows into events_to_process", len(df_new))
    except IntegrityError as dup:
        logger.warning("Duplicates skipped: %s", dup.orig.args[1])
    finally:
        eng.dispose()

    return _resp(200, "Athens crawl completed successfully")


def _resp(status: int, msg: str) -> dict:
    """Return an API‑Gateway‑friendly JSON response."""
    return {"statusCode": status, "body": json.dumps(msg)}
