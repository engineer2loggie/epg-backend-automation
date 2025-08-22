#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
REQUEST_TIMEOUT = (10, 180)  # (connect, read)

# ----------------------- Logging ----------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg")

# ----------------------- Env --------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls.strip() else DEFAULT_EPG_URLS[:]

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
    Accepts:
      YYYYMMDDHHMMSSZ
      YYYYMMDDHHMMSS Z
      YYYYMMDDHHMMSS+HHMM
      YYYYMMDDHHMMSS+HH:MM
      YYYYMMDDHHMMSS      (assume UTC)
    Returns aware UTC datetime or None.
    """
    if not raw:
        return None
    s = raw.strip()

    # Remove space before TZ if present
    if " " in s and (s.endswith("Z") or s[-5:s-4] in ["+", "-"]):
        parts = s.rsplit(" ", 1)
        s = "".join(parts)

    # Normalize +HH:MM -> +HHMM
    if len(s) >= 6 and s[-3:] != "Z" and s[-3] == ":" and s[-6] in ["+", "-"]:
        s = s[:-3] + s[-2:]

    # Add UTC if no tz
    if len(s) == 14:  # YYYYMMDDHHMMSS
        s += "+0000"

    # Handle trailing Z
    if s.endswith("Z") and len(s) == 15:
        s = s[:-1] + "+0000"

    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def time_windows_overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start <= b_end and a_end >= b_start


def open_xml_stream(resp: requests.Response, url: str):
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    is_gz = url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct
    return gzip.GzipFile(fileobj=resp.raw) if is_gz else resp.raw


def preferred_text(elem: ET.Element, tag: str) -> Optional[str]:
    # First non-empty text among possibly many localized tags
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

    all_channels: Dict[str, dict] = {}     # id -> channel row
    program_map: Dict[str, dict] = {}      # unique programme id -> programme row

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start", "end"))
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
                            icon_url = elem.find("icon").get("src") if elem.find("icon") is not None else None
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
                            elem.clear(); continue

                        # Only keep items that overlap now..+24h
                        if not time_windows_overlap(start_dt, end_dt, now_utc, horizon_utc):
                            elem.clear(); continue

                        title = preferred_text(elem, "title") or "No Title"
                        desc = preferred_text(elem, "desc")

                        # >>> NEW: include END in the ID to reduce natural collisions
                        prog_id = f"{ch_id}_{start_dt.strftime('%Y%m%d%H%M%S')}_{end_dt.strftime('%Y%m%d%H%M%S')}"

                        cand = {
                            "id": prog_id,
                            "channel_id": ch_id,
                            "start_time": start_dt.isoformat(),
                            "end_time": end_dt.isoformat(),
                            "title": title,
                            "description": desc
                        }

                        # >>> NEW: de-dup within the same payload to avoid 21000 errors
                        prev = program_map.get(prog_id)
                        if prev is None:
                            program_map[prog_id] = cand
                            kept_programs += 1
                        else:
                            # pick the "better" one: non-empty title > longer desc > keep existing
                            prev_title = (prev.get("title") or "").strip()
                            prev_desc = (prev.get("description") or "") or ""
                            cand_title = (cand.get("title") or "").strip()
                            cand_desc = (cand.get("description") or "") or ""

                            replace = False
                            if prev_title == "No Title" and cand_title != "No Title":
                                replace = True
                            elif len(cand_desc) > len(prev_desc):
                                replace = True

                            if replace:
                                program_map[prog_id] = cand

                        elem.clear()
                        if kept_programs % 5000 == 0:
                            root.clear()
                        continue

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

    # Ensure all referenced channel_ids exist
    referenced_ids = {p["channel_id"] for p in program_map.values()}
    missing = referenced_ids.difference(all_channels.keys())
    if missing:
        log.warning("Programs reference %d channels missing in <channel> list. Creating placeholders.", len(missing))
        for ch_id in missing:
            all_channels[ch_id] = {"id": ch_id, "display_name": ch_id, "icon_url": None}

    # Upserts
    if all_channels:
        upsert_channels(sb, list(all_channels.values()))
    else:
        log.warning("No channels parsed to upsert.")

    programs = list(program_map.values())
    log.info("Deduplicated programs total: %d (dropped %d duplicates).", len(programs), len(program_map) - len(programs))

    if programs:
        programs.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_programs(sb, programs)
    else:
        log.warning("No programs kept for next 24h; check timestamp parsing and window.")

    cleanup_old_programs(sb, older_than_utc=datetime.now(timezone.utc) - timedelta(hours=24))
    log.info("Done. Channels upserted: %d; Programs upserted: %d", len(all_channels), len(programs))


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
