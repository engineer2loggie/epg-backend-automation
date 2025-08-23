#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Open-EPG SpreadsheetML -> Supabase loader

- Parses SpreadsheetML columns:
  E=channel_id/name, F=start, G=stop, H=description, L=title
  (Ignores A-D entirely. Carries E downward when empty.)
- Title strictly from column L (no <sub-title> fallback ever).
- 12h rolling window computed in local tz (DST-aware) then converted to UTC.
- Deletes programs older than 12h (by end_time).
- Works with .xml and .xml.gz feeds.
"""

from __future__ import annotations
import os, sys, gzip, time, logging, itertools, html
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

# Default to the two PR files; override with OPEN_EPG_URLS env (comma-separated)
DEFAULT_EPG_URLS = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]

REQUEST_TIMEOUT = (10, 180)   # (connect, read)
BATCH_CHANNELS  = 2000
BATCH_PROGRAMS  = 1000
MAX_RETRIES     = 4

# Window & timezone (DST aware). For PR you can set America/Puerto_Rico.
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))  # required: 12h
WINDOW_TZ    = os.environ.get("WINDOW_TZ", "America/Puerto_Rico")

# Skip rows where title (L) is empty? Default keep them.
SKIP_EMPTY_TITLES = os.environ.get("SKIP_EMPTY_TITLES", "0") in ("1","true","True")

# Debug samples
DEBUG_SAMPLE_CHANNELS = int(os.environ.get("DEBUG_SAMPLE_CHANNELS", "10"))
DEBUG_SAMPLE_PROGRAMS = int(os.environ.get("DEBUG_SAMPLE_PROGRAMS", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-epg")

SUPABASE_URL         = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("OPEN_EPG_URLS", "")
EPG_URLS  = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_EPG_URLS)

# ----------------------- Helpers ----------------------

def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, size))
        if not block:
            return
        yield block

def localname(tag: str) -> str:
    if not tag:
        return tag
    if tag[0] == "{":
        return tag.split("}", 1)[1]
    return tag

def open_xml_stream(resp: requests.Response, url: str):
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    gz = url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct
    return gzip.GzipFile(fileobj=resp.raw) if gz else resp.raw

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    """Parse 'YYYYMMDDHHMMSS +ZZZZ' (or variants) -> aware UTC datetime."""
    if not raw:
        return None
    s = raw.strip()
    # fix accidental double-space before tz
    if " " in s:
        a, b = s.rsplit(" ", 1)
        # if b looks like +HHMM
        if len(b) == 5 and b[0] in "+-" and b[1:].isdigit():
            s = a + b
    # normalize +HH:MM to +HHMM
    if len(s) >= 6 and not s.endswith("Z") and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]
    # Z -> +0000
    if s.endswith("Z"):
        s = s[:-1] + "+0000"
    # add UTC if no tz present (assume UTC)
    if len(s) == 14:
        s += "+0000"
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def get_cell_text(cell: ET.Element) -> str:
    """Extract text from SpreadsheetML <Cell><Data>…</Data></Cell>."""
    # text can be directly inside Cell, but usually under Data
    data = None
    for ch in list(cell):
        if localname(ch.tag).lower() == "data":
            data = ch
            break
    txt = "".join(data.itertext()).strip() if data is not None else "".join(cell.itertext()).strip()
    return html.unescape(txt)

def row_cells_to_map(row: ET.Element) -> Dict[int, str]:
    """
    Convert a SpreadsheetML <Row> into a 1-based column map, handling ss:Index gaps.
    Returns {col_index: text}
    """
    cols: Dict[int, str] = {}
    col_idx = 0
    for cell in list(row):
        if localname(cell.tag).lower() != "cell":
            continue
        # support ss:Index or any *:Index attr
        explicit_idx = None
        for k, v in cell.attrib.items():
            if localname(k).lower() == "index":
                try:
                    explicit_idx = int(v)
                except Exception:
                    pass
                break
        if explicit_idx is not None and explicit_idx > 0:
            col_idx = explicit_idx
        else:
            col_idx += 1
        cols[col_idx] = get_cell_text(cell)
    return cols

# ----------------------- Supabase ---------------------

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
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch)
                break
            except APIError as e:
                msg = str(e)
                need_split = ("21000" in msg or "duplicate key value" in msg or "500" in msg or "413" in msg or "Payload" in msg)
                if need_split and len(batch) > 1:
                    mid = len(batch)//2
                    queue.insert(0, batch[mid:])
                    queue.insert(0, batch[:mid])
                    log.warning("Splitting %s batch (%d) due to error: %s", table, len(batch), msg)
                    break
                if attempt == MAX_RETRIES:
                    log.error("Giving up on %s batch (%d): %s", table, len(batch), msg)
                else:
                    sleep_s = attempt * 0.6
                    log.warning("Retry %d/%d for %s (%d rows) in %.1fs: %s", attempt, MAX_RETRIES, table, len(batch), sleep_s, msg)
                    time.sleep(sleep_s)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    sleep_s = attempt * 0.6
                    log.warning("Retry %d/%d for %s (%d rows) in %.1fs (unexpected): %s", attempt, MAX_RETRIES, table, len(batch), sleep_s, e)
                    time.sleep(sleep_s)
    log.info("Upserted %d rows into %s.", total, table)

# ----------------------- Window ----------------------

def compute_window_utc() -> Tuple[datetime, datetime, datetime]:
    """
    Return (now_local, now_utc, horizon_utc).
    - now_local in WINDOW_TZ (DST aware)
    - now_utc, horizon_utc in UTC
    """
    lt = ZoneInfo(WINDOW_TZ)
    now_local = datetime.now(lt)
    horizon_local = now_local + timedelta(hours=WINDOW_HOURS)
    now_utc = now_local.astimezone(timezone.utc)
    horizon_utc = horizon_local.astimezone(timezone.utc)
    return now_local, now_utc, horizon_utc

# ----------------------- Core ingest ------------------

def parse_spreadsheet_feed(url: str,
                           now_utc: datetime,
                           horizon_utc: datetime) -> Tuple[Dict[str, dict], List[dict]]:
    """
    Parse a SpreadsheetML table:
      E=5 channel, F=6 start, G=7 stop, H=8 description, L=12 title
    Carry E downward when empty. Ignore rows with no start.
    Keep only rows overlapping [now_utc, horizon_utc].
    """
    CHANNEL_COL = 5
    START_COL   = 6
    STOP_COL    = 7
    DESC_COL    = 8
    TITLE_COL   = 12

    channels: Dict[str, dict] = {}
    programs: List[dict] = []

    current_channel: Optional[str] = None
    total_rows = 0
    kept_rows  = 0
    titled     = 0

    log.info("Fetching EPG (SpreadsheetML): %s", url)
    try:
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status_code == 404:
                log.warning("EPG URL 404: %s (skipping)", url)
                return channels, programs
            resp.raise_for_status()
            stream = open_xml_stream(resp, url)
            context = ET.iterparse(stream, events=("start", "end"))
            _, root = next(context)

            sample_prog: List[dict] = []
            sample_channels: List[Tuple[str,str]] = []

            for ev, el in context:
                if ev != "end":
                    continue
                if localname(el.tag).lower() != "row":
                    el.clear()
                    continue

                total_rows += 1
                cols = row_cells_to_map(el)

                # Skip A-D always by design. We only touch from E onward.
                ch_txt = cols.get(CHANNEL_COL, "").strip()
                if ch_txt:
                    current_channel = html.unescape(ch_txt)

                start_raw = cols.get(START_COL, "").strip()
                if not start_raw:
                    el.clear()
                    continue

                # parse times
                s = parse_xmltv_datetime(start_raw)
                e = parse_xmltv_datetime(cols.get(STOP_COL, "").strip() or "")
                if s is None:
                    el.clear()
                    continue
                if e is None or e <= s:
                    # fallback: 30 minutes if no stop given or bad
                    e = (s + timedelta(minutes=30))

                # window filter (overlap)
                if not (s <= horizon_utc and e >= now_utc):
                    el.clear()
                    continue

                ch_id = (current_channel or "").strip()
                if not ch_id:
                    el.clear()
                    continue

                # columns for description/title
                desc  = html.unescape(cols.get(DESC_COL, "").strip())
                title = html.unescape(cols.get(TITLE_COL, "").strip())  # STRICT: title only from L

                if not title:
                    if SKIP_EMPTY_TITLES:
                        el.clear()
                        continue
                else:
                    titled += 1

                # channels map (first time we see it)
                if ch_id not in channels:
                    channels[ch_id] = {
                        "id": ch_id,
                        "display_name": ch_id,
                        "icon_url": None
                    }
                    if len(sample_channels) < DEBUG_SAMPLE_CHANNELS:
                        sample_channels.append((ch_id, ch_id))

                # add program
                pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                programs.append({
                    "id": pid,
                    "channel_id": ch_id,
                    "start_time": s.isoformat(),  # UTC
                    "end_time": e.isoformat(),    # UTC
                    "title": title or None,
                    "description": desc or None,
                })
                kept_rows += 1

                # debug sample
                if len(sample_prog) < DEBUG_SAMPLE_PROGRAMS:
                    sample_prog.append({
                        "channel_id": ch_id,
                        "start": s.isoformat(),
                        "title": title,
                        "desc_len": len(desc or "")
                    })

                el.clear()
                # free memory every so often (rows can be huge)
                if (kept_rows % 10000) == 0:
                    root.clear()

            # log samples
            if sample_channels:
                log.info("SAMPLE channels (%d):", len(sample_channels))
                for cid, nm in sample_channels:
                    log.info("  id=%s name='%s'", cid, nm)
            if sample_prog:
                log.info("SAMPLE programmes (%d):", len(sample_prog))
                for p in sample_prog:
                    log.info("  ch=%s start=%s title=%r desc_len=%d",
                             p["channel_id"], p["start"], p["title"], p["desc_len"])

            ratio = (titled / kept_rows) if kept_rows else 0.0
            log.info("Parsed %s: rows_seen=%d, programs_kept=%d, titled_ratio=%.3f",
                     url, total_rows, kept_rows, ratio)

            return channels, programs

    except requests.exceptions.RequestException as e:
        log.error("HTTP error for %s: %s", url, e)
    except ET.ParseError as e:
        log.error("XML parse error for %s: %s", url, e)
    except Exception as e:
        log.exception("Unexpected error for %s: %s", url, e)

    return {}, []

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
            time.sleep(0.8 * attempt)

def fetch_and_process(sb: Client, urls: List[str]):
    # 12h DST-aware window
    now_local, now_utc, horizon_utc = compute_window_utc()
    log.info("Windowing: ON (local tz=%s) -> UTC window: %s -> %s",
             now_local.tzinfo, now_utc.isoformat(), horizon_utc.isoformat())

    channels_all: Dict[str, dict] = {}
    programs_all: Dict[str, dict] = {}

    for url in urls:
        chs, progs = parse_spreadsheet_feed(url, now_utc, horizon_utc)
        for cid, crow in chs.items():
            if cid not in channels_all:
                channels_all[cid] = crow
        for row in progs:
            pid = row["id"]
            # de-dupe by preferring a row with a non-empty title or longer description
            keep = programs_all.get(pid)
            if keep is None:
                programs_all[pid] = row
            else:
                t_old = (keep.get("title") or "")
                t_new = (row.get("title") or "")
                d_old = (keep.get("description") or "")
                d_new = (row.get("description") or "")
                replace = False
                if not t_old and t_new:
                    replace = True
                elif len(d_new) > len(d_old):
                    replace = True
                if replace:
                    programs_all[pid] = row

    # Upserts
    if channels_all:
        ch_rows = list(channels_all.values())
        log.info("Upserting %d channels …", len(ch_rows))
        upsert_with_retry(sb, "channels", ch_rows, conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels collected.")

    prog_rows = list(programs_all.values())
    prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
    log.info("Programs to upsert (deduped): %d", len(prog_rows))
    if prog_rows:
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programs parsed for upsert.")

    # Quick sanity (how many have title/desc)
    try:
        res_total = sb.table("programs").select("id", count="exact").execute()
        total = getattr(res_total, "count", None) or 0
        with_title = sb.table("programs").select("id").not_.is_("title", "null").execute()
        with_desc  = sb.table("programs").select("id").not_.is_("description", "null").execute()
        log.info("DB sanity: total=%d, with_title=%d, with_desc=%d",
                 total, len(getattr(with_title, "data", [])), len(getattr(with_desc, "data", [])))
    except Exception as e:
        log.warning("Sanity check failed: %s", e)

    # Delete rows older than 12h (by end_time).  Compute in local tz with DST, then convert to UTC.
    try:
        cutoff_local = now_local - timedelta(hours=WINDOW_HOURS)
        cutoff_utc   = cutoff_local.astimezone(timezone.utc)
        sb.table("programs").delete().lt("end_time", cutoff_utc.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s (UTC)", cutoff_utc.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # Refresh MV
    refresh_next_12h_mv(sb)

# ----------------------- Entrypoint -------------------

def main() -> int:
    log.info("Open-EPG ingest (SpreadsheetML). WINDOW_HOURS=%d, WINDOW_TZ=%s, SKIP_EMPTY_TITLES=%s",
             WINDOW_HOURS, WINDOW_TZ, SKIP_EMPTY_TITLES)
    if not EPG_URLS:
        log.warning("No EPG URLs provided (OPEN_EPG_URLS env). Using defaults.")
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
