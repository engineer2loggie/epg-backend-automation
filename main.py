#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Refactored XMLTV -> Supabase ingester
- Streams & parses .xml or .xml.gz
- Normalizes XMLTV timestamps
- Writes channels + next-24h programmes to Supabase
- Deletes programmes that ended >24h ago

ENV:
  SUPABASE_URL           (required)
  SUPABASE_SERVICE_KEY   (required)
  EPG_URLS               (optional, comma-separated; defaults to epgshare01 ALL_SOURCES)
"""

from __future__ import annotations
import os
import sys
import io
import gzip
import time
import logging
import itertools
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Tuple

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client

# ----------------------- Config -----------------------

DEFAULT_EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
]

BATCH_SIZE_CHANNELS = 5000
BATCH_SIZE_PROGRAMS = 5000
REQUEST_TIMEOUT = (10, 180)  # (connect, read) seconds

# ----------------------- Logging ----------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("epg")

# ----------------------- Env --------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("EPG_URLS", "")
if _raw_urls.strip():
    EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()]
else:
    EPG_URLS = DEFAULT_EPG_URLS[:]

# ----------------------- Helpers ----------------------


def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, size))
        if not block:
            return
        yield block


def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    """
    Accepts common XMLTV forms:
      - YYYYMMDDHHMMSS Z         (note the space before offset)
      - YYYYMMDDHHMMSSZ          (Z)
      - YYYYMMDDHHMMSS+HH:MM
      - YYYYMMDDHHMMSS+HHMM
      - YYYYMMDDHHMMSS           (assume UTC)
    Returns aware UTC datetime, or None.
    """
    if not raw:
        return None

    s = raw.strip()

    # Remove whitespace between datetime and tz if present, e.g. "20250820103000 +0000"
    if " " in s and (s.endswith("Z") or s[-5] in ["+", "-"]):
        parts = s.rsplit(" ", 1)
        s = "".join(parts)

    # Normalize +HH:MM -> +HHMM
    if len(s) >= 5 and (s[-3] == ":" and (s[-6] in ["+", "-"])):
        s = s[:-3] + s[-2:]

    # Add UTC if missing tz entirely
    if len(s) == 14:  # YYYYMMDDHHMMSS
        s = s + "+0000"

    # Handle trailing Z
    if s.endswith("Z") and len(s) == 15:  # 14 + "Z"
        s = s[:-1] + "+0000"

    try:
        # Expect exactly 'YYYYMMDDHHMMSS±HHMM'
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def time_windows_overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    """True if [a_start, a_end] intersects [b_start, b_end]."""
    return a_start <= b_end and a_end >= b_start


def open_xml_stream(resp: requests.Response, url: str):
    """
    Returns a file-like object suitable for ET.iterparse().
    Handles .gz by wrapping in gzip.GzipFile.
    """
    # Ensure urllib3 will decompress if the server used Content-Encoding
    resp.raw.decode_content = True

    ct = (resp.headers.get("Content-Type") or "").lower()
    is_gz = url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct

    raw_stream = resp.raw  # file-like

    if is_gz:
        # Wrap in GzipFile to get raw XML stream
        return gzip.GzipFile(fileobj=raw_stream)
    else:
        return raw_stream


def preferred_text(elem: ET.Element, tag: str) -> Optional[str]:
    """
    Return the first non-empty text for the given tag under elem.
    (XMLTV can include multiple <title> / <desc> with lang attributes.)
    """
    for child in elem.findall(tag):
        if child.text and child.text.strip():
            return child.text.strip()
    return None


# ----------------------- Supabase ---------------------


def init_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)
    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("Connected to Supabase.")
        return client
    except Exception as e:
        log.exception("Failed to create Supabase client: %s", e)
        sys.exit(1)


def upsert_channels(sb: Client, rows: List[dict]):
    total = 0
    for batch in chunked(rows, BATCH_SIZE_CHANNELS):
        sb.table("channels").upsert(batch, on_conflict="id").execute()
        total += len(batch)
    log.info("Upserted %d channels.", total)


def upsert_programs(sb: Client, rows: List[dict]):
    total = 0
    for batch in chunked(rows, BATCH_SIZE_PROGRAMS):
        sb.table("programs").upsert(batch, on_conflict="id").execute()
        total += len(batch)
    log.info("Upserted %d programs.", total)


def cleanup_old_programs(sb: Client, older_than_utc: datetime):
    try:
        sb.table("programs").delete().lt("end_time", older_than_utc.isoformat()).execute()
        log.info("Deleted programs with end_time < %s.", older_than_utc.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)


# ----------------------- Core ingest ------------------


def fetch_and_process_epg(sb: Client, urls: List[str]):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=24)

    # Accumulators across all files
    all_channels: Dict[str, dict] = {}  # id -> row
    programs: List[dict] = []

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start", "end"))
                # Advance to get the root element
                _, root = next(context)

                found_channels = 0
                found_programs = 0
                kept_programs = 0

                for event, elem in context:
                    if event != "end":
                        continue

                    tag = elem.tag

                    if tag == "channel":
                        ch_id = elem.get("id")
                        if ch_id:
                            display_name = preferred_text(elem, "display-name") or ch_id
                            icon_url = None
                            icon_node = elem.find("icon")
                            if icon_node is not None:
                                icon_url = icon_node.get("src")
                            if ch_id not in all_channels:
                                all_channels[ch_id] = {
                                    "id": ch_id,
                                    "display_name": display_name,
                                    "icon_url": icon_url,
                                }
                                found_channels += 1
                        elem.clear()
                        continue

                    if tag == "programme":
                        found_programs += 1
                        ch_id = elem.get("channel")
                        start_dt = parse_xmltv_datetime(elem.get("start"))
                        end_dt = parse_xmltv_datetime(elem.get("stop"))

                        if not (ch_id and start_dt and end_dt):
                            elem.clear()
                            continue

                        # Keep only if overlaps [now, now+24h]
                        if not time_windows_overlap(start_dt, end_dt, now_utc, horizon_utc):
                            elem.clear()
                            continue

                        title = preferred_text(elem, "title") or "No Title"
                        desc = preferred_text(elem, "desc")

                        # Programme unique id: channel + start timestamp
                        prog_id = f"{ch_id}_{start_dt.strftime('%Y%m%d%H%M%S')}"

                        programs.append({
                            "id": prog_id,
                            "channel_id": ch_id,
                            "start_time": start_dt.isoformat(),
                            "end_time": end_dt.isoformat(),
                            "title": title,
                            "description": desc
                        })
                        kept_programs += 1

                        elem.clear()
                        # Occasionally clear the root to free memory
                        if kept_programs % 5000 == 0:
                            root.clear()
                        continue

                    # For any other end-tag, just clear to save memory
                    elem.clear()

                log.info(
                    "Parsed file done: channels=%d (new), programs_found=%d, programs_kept_24h=%d",
                    found_channels, found_programs, kept_programs
                )

        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

    # Ensure all referenced channel_ids exist (in case some <programme> lacked a <channel> block)
    referenced_ids = {p["channel_id"] for p in programs}
    missing = referenced_ids.difference(all_channels.keys())
    if missing:
        log.warning("Programs reference %d channels missing in <channel> list. Creating placeholders.", len(missing))
        for ch_id in missing:
            all_channels[ch_id] = {
                "id": ch_id,
                "display_name": ch_id,
                "icon_url": None,
            }

    # Upserts
    if all_channels:
        upsert_channels(sb, list(all_channels.values()))
    else:
        log.warning("No channels parsed to upsert.")

    if programs:
        # Sort (optional) for nicer batching by channel/time
        programs.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_programs(sb, programs)
    else:
        log.warning("No programs kept for next 24h; check timestamp parsing and window.")

    # Cleanup: delete programmes that ended more than 24h ago
    cleanup_old_programs(sb, older_than_utc=now_utc - timedelta(hours=24))

    log.info("Done. Channels total upserted: %d; Programs total upserted: %d",
             len(all_channels), len(programs))


# ----------------------- Entrypoint -------------------


def main() -> int:
    log.info("EPG ingest starting. URLs: %s", ", ".join(EPG_URLS))
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_epg(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
