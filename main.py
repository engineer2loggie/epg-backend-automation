#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, time, logging, itertools, random
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Dict, Optional, Tuple

import requests
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

# Open-EPG Puerto Rico (SpreadsheetML) – you can append more country files later
DEFAULT_OPEN_EPG_URLS = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]

# Window & timezone (DST-aware). We parse only NOW → NOW+12h and delete < NOW-12h.
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))
WINDOW_TZ = os.environ.get("WINDOW_TZ", "America/Puerto_Rico")

# Batching / retries
BATCH_CHANNELS  = 1000
BATCH_PROGRAMS  = 800
MAX_RETRIES     = 4
REQUEST_TIMEOUT = (10, 180)

# Debugging samples
DEBUG_SAMPLE_ROWS = int(os.environ.get("DEBUG_SAMPLE_ROWS", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-epg")

SUPABASE_URL         = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("OPEN_EPG_URLS", "")
OPEN_EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_OPEN_EPG_URLS)

if WINDOW_HOURS <= 0:
    WINDOW_HOURS = 12  # sane fallback

# ----------------------- Helpers ----------------------

def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, size))
        if not block:
            return
        yield block

def rand_jitter() -> float:
    return 0.25 + random.random() * 0.75

def localname(tag: str) -> str:
    if not tag:
        return tag
    if tag[0] == "{":
        return tag.split("}", 1)[1]
    return tag

def attr_local(attrs: Dict[str, str], wanted: str) -> Optional[str]:
    # find attribute by localname (e.g., ss:Index)
    for k, v in attrs.items():
        if localname(k).lower() == wanted.lower():
            return v
    return None

def parse_epg_dt(raw: Optional[str]) -> Optional[datetime]:
    """
    Parse strings like '20250822223000 +0000', '20250822223000+0000', or '20250822223000Z'.
    Return timezone-aware UTC datetime.
    """
    if not raw:
        return None
    s = raw.strip()
    # collapse accidental space before tz
    if " " in s:
        a, b = s.rsplit(" ", 1)
        if (len(b) in (5, 6)) and (b[0] in "+-0123456789Z"):
            s = a + b
    if s.endswith("Z"):
        s = s[:-1] + "+0000"
    # add tz if missing
    if len(s) == 14:
        s += "+0000"
    # normalize +HH:MM → +HHMM
    if len(s) >= 19 and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def init_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("✅ Connected to Supabase.")
        return sb
    except Exception as e:
        log.exception("Failed to create Supabase client: %s", e)
        sys.exit(1)

def upsert_with_retry(sb: Client, table: str, rows: List[dict], conflict: str, base_batch: int):
    total = 0
    queue: List[List[dict]] = list(chunked(rows, base_batch))

    while queue:
        batch = queue.pop(0)

        # De-dup rows by id inside the batch (prefer non-empty title, longer desc)
        if conflict == "id":
            dedup: Dict[str, dict] = {}
            for r in batch:
                k = r.get("id")
                if not k:
                    continue
                keep = dedup.get(k)
                if keep is None:
                    dedup[k] = r
                else:
                    # prefer non-empty title; if both empty, prefer longer description
                    t0 = (keep.get("title") or "").strip()
                    t1 = (r.get("title") or "").strip()
                    d0 = keep.get("description") or ""
                    d1 = r.get("description") or ""
                    replace = (not t0 and t1) or (len(d1) > len(d0))
                    if replace:
                        dedup[k] = r
            batch = list(dedup.values())

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch)
                break
            except APIError as e:
                msg = str(e)
                need_split = (
                    "21000" in msg or
                    "duplicate key value violates" in msg or
                    "500" in msg or
                    "413" in msg or
                    "Payload" in msg
                )
                if need_split and len(batch) > 1:
                    mid = len(batch) // 2
                    queue.insert(0, batch[mid:])
                    queue.insert(0, batch[:mid])
                    log.warning("Splitting %s batch (%d) due to error: %s", table, len(batch), msg)
                    break
                if attempt == MAX_RETRIES:
                    log.error("Giving up on %s batch (%d): %s", table, len(batch), msg)
                else:
                    sleep_s = attempt * rand_jitter()
                    log.warning("Retry %d/%d for %s (%d rows) in %.2fs: %s",
                                attempt, MAX_RETRIES, table, len(batch), sleep_s, msg)
                    time.sleep(sleep_s)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    sleep_s = attempt * rand_jitter()
                    log.warning("Retry %d/%d for %s (%d rows) in %.2fs (unexpected): %s",
                                attempt, MAX_RETRIES, table, len(batch), sleep_s, e)
                    time.sleep(sleep_s)

    log.info("Upserted %d rows into %s.", total, table)

def count_programs_in_window(sb: Client, start_utc: datetime, end_utc: datetime) -> int:
    try:
        res = sb.table("programs")\
            .select("id", count="exact")\
            .gte("end_time", start_utc.isoformat())\
            .lte("start_time", end_utc.isoformat())\
            .execute()
        return getattr(res, "count", None) or 0
    except Exception as e:
        log.warning("Count query failed: %s", e)
        return -1

def refresh_next_12h_mv(sb: Client) -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Refreshing MV via RPC: refresh_programs_next_12h …")
            sb.rpc("refresh_programs_next_12h").execute()
            log.info("✅ MV refreshed.")
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.warning("RPC refresh_programs_next_12h failed after %d attempts: %s", attempt, e)
                return
            sleep_s = attempt * rand_jitter()
            log.warning("Retry %d/%d for refresh_programs_next_12h in %.2fs: %s",
                        attempt, MAX_RETRIES, sleep_s, e)
            time.sleep(sleep_s)

# ----------------------- SpreadsheetML parsing ----------------------

# Columns we care about (1-based)
COL_E_CHANNEL      = 5   # /programme/@channel  (sticky)
COL_F_START        = 6   # start time (e.g., 20250822223000 +0000)
COL_G_STOP         = 7   # stop time
COL_H_DESCRIPTION  = 8   # /programme/desc
COL_L_TITLE        = 12  # /programme/title

def row_to_cells(row_el: ET.Element) -> Dict[int, str]:
    """
    Convert a <Row> to a dict: {col_index1: text, ...}, honoring ss:Index gaps.
    """
    cells: Dict[int, str] = {}
    col_idx = 0
    for cell in row_el:
        if localname(cell.tag).lower() != "cell":
            continue
        jump = attr_local(cell.attrib, "Index")
        if jump:
            try:
                col_idx = int(jump) - 1  # ss:Index is 1-based, and we'll increment immediately
            except Exception:
                pass
        col_idx += 1
        # find first <Data> and collect all text
        text = ""
        for data in cell:
            if localname(data.tag).lower() == "data":
                # concatenate inner text (SpreadsheetML sometimes nests)
                text = "".join(data.itertext()).strip()
                break
        if text:
            cells[col_idx] = text
    return cells

def parse_spreadsheet_urls(urls: List[str], window_start_utc: datetime, window_end_utc: datetime) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    """
    Parse SpreadsheetML files and return (channels, programs).
    channels:  {id -> {id, display_name, icon_url}}
    programs:  {pid -> {id, channel_id, start_time, end_time, title, description}}
    """
    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}

    for url in urls:
        log.info("Fetching EPG (SpreadsheetML): %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                resp.raw.decode_content = True
                context = ET.iterparse(resp.raw, events=("start", "end"))
                _, root = next(context)

                total_rows = 0
                kept_rows  = 0
                titled     = 0

                current_channel: Optional[str] = None

                for ev, el in context:
                    if ev != "end":
                        continue
                    if localname(el.tag).lower() != "row":
                        continue

                    total_rows += 1
                    cells = row_to_cells(el)

                    # Sticky channel id
                    ch = cells.get(COL_E_CHANNEL) or current_channel
                    if cells.get(COL_E_CHANNEL):
                        current_channel = ch

                    # Only process "programme rows": need ch, start, stop at minimum
                    start_raw = cells.get(COL_F_START)
                    stop_raw  = cells.get(COL_G_STOP)
                    if not (ch and start_raw and stop_raw):
                        el.clear()
                        if (total_rows % 5000) == 0:
                            root.clear()
                        continue

                    s = parse_epg_dt(start_raw)
                    e = parse_epg_dt(stop_raw)
                    if not (s and e):
                        el.clear()
                        if (total_rows % 5000) == 0:
                            root.clear()
                        continue

                    # Keep only rows overlapping [window_start_utc, window_end_utc]
                    if not (s <= window_end_utc and e >= window_start_utc):
                        el.clear()
                        if (total_rows % 5000) == 0:
                            root.clear()
                        continue

                    title = (cells.get(COL_L_TITLE) or "").strip()
                    desc  = (cells.get(COL_H_DESCRIPTION) or "").strip()
                    if title:
                        titled += 1

                    # Build rows
                    if ch not in channels:
                        channels[ch] = {"id": ch, "display_name": ch, "icon_url": None}

                    pid = f"{ch}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                    programs[pid] = {
                        "id": pid,
                        "channel_id": ch,
                        "start_time": s.isoformat(),
                        "end_time": e.isoformat(),
                        "title": title or None,           # store NULL if empty
                        "description": desc or None       # store NULL if empty
                    }
                    kept_rows += 1

                    # Done with this Row
                    el.clear()
                    if (total_rows % 5000) == 0:
                        root.clear()

                ratio = (titled / kept_rows) if kept_rows else 0.0
                log.info("Parsed %s: rows_seen=%d, programs_kept=%d, titled_ratio=%.3f",
                         url, total_rows, kept_rows, ratio)

        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

    return channels, programs

# ----------------------- Core ingest ------------------

def main() -> int:
    log.info("Open-EPG ingest (SpreadsheetML). WINDOW_HOURS=%d, WINDOW_TZ=%s, SKIP_EMPTY_TITLES=False",
             WINDOW_HOURS, WINDOW_TZ)

    sb = init_supabase()

    # Compute the window (DST-aware local → UTC)
    try:
        loc_tz = ZoneInfo(WINDOW_TZ)
    except Exception:
        loc_tz = ZoneInfo("UTC")
    now_local = datetime.now(loc_tz)
    horizon_local = now_local + timedelta(hours=WINDOW_HOURS)
    now_utc = now_local.astimezone(timezone.utc)
    horizon_utc = horizon_local.astimezone(timezone.utc)

    log.info("Windowing: ON (local tz=%s) -> UTC window: %s -> %s",
             WINDOW_TZ, now_utc.isoformat(), horizon_utc.isoformat())

    # Parse SpreadsheetML feeds
    channels, programs = parse_spreadsheet_urls(OPEN_EPG_URLS, now_utc, horizon_utc)

    # Show a few samples
    if programs:
        sample = list(programs.values())[:DEBUG_SAMPLE_ROWS]
        log.info("SAMPLE programmes (%d):", len(sample))
        for r in sample:
            t = (r.get("title") or "")
            dl = len(r.get("description") or "")
            log.info("  ch=%s start=%s end=%s title=%r desc_len=%d",
                     r["channel_id"], r["start_time"], r["end_time"], t, dl)
    else:
        log.warning("No programs parsed for upsert.")

    # Upsert channels
    if channels:
        log.info("Upserting %d channels …", len(channels))
        upsert_with_retry(sb, "channels", list(channels.values()), conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels collected.")

    # Upsert programs
    prog_rows = list(programs.values())
    log.info("Programs to upsert (deduped): %d", len(prog_rows))
    if prog_rows:
        # stable order (not required, helpful for logs)
        prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programs parsed for upsert.")

    # Sanity check in the 12h window
    cnt = count_programs_in_window(sb, now_utc, horizon_utc)
    if cnt >= 0:
        log.info("✅ Supabase now has %d programs in the 12h window.", cnt)

    # Delete rows older than 12 hours (relative to NOW)
    cutoff = now_utc - timedelta(hours=12)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s (UTC)", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # Refresh MV (12h)
    refresh_next_12h_mv(sb)

    log.info("Finished.")
    return 0

# ----------------------- Entrypoint -------------------

if __name__ == "__main__":
    sys.exit(main())
