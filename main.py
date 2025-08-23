#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Open-EPG -> Supabase loader
- 12h rolling window (computed in local tz with DST, then converted to UTC)
- Deletes programs older than 12h
- Title strictly from <title> (no <sub-title> fallback)
"""

from __future__ import annotations
import os, sys, gzip, time, logging, itertools
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Dict, Optional, Tuple
from zoneinfo import ZoneInfo
import html

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

# Default Open-EPG Puerto Rico files; override in env OPEN_EPG_URLS (comma-separated)
DEFAULT_EPG_URLS = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]

REQUEST_TIMEOUT = (10, 180)  # (connect, read)
BATCH_CHANNELS = 2000
BATCH_PROGRAMS = 1000
MAX_RETRIES = 4

# Window & timezone (DST aware)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))  # you asked for 12h
WINDOW_TZ = os.environ.get("WINDOW_TZ", "America/New_York")  # computed in local tz, then UTC
SKIP_EMPTY_TITLES = os.environ.get("SKIP_EMPTY_TITLES", "0") in ("1", "true", "True")

# Preferred language order for <title>/<desc>
PREFER_LANGS = [p.strip().lower() for p in os.environ.get("PREFER_LANGS", "es-pr,es,en").split(",") if p.strip()]

# Debug sampler limits
DEBUG_SAMPLE_CHANNELS = int(os.environ.get("DEBUG_SAMPLE_CHANNELS", "10"))
DEBUG_SAMPLE_PROGRAMS = int(os.environ.get("DEBUG_SAMPLE_PROGRAMS", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-epg")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("OPEN_EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_EPG_URLS)

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

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    """Parse XMLTV datetime of the form YYYYMMDDHHMMSS +ZZZZ, Z, or no tz.
    Return timezone-aware UTC datetime."""
    if not raw:
        return None
    s = raw.strip()

    # Trim double-space before offset if present ("...  +0000")
    if " " in s:
        # keep the last token as tz, if it looks like +HHMM or -HHMM
        a, b = s.rsplit(" ", 1)
        if len(b) in (5,) and (b[0] in "+-") and b[1:].isdigit():
            s = a + b

    # Normalize "+HH:MM" to "+HHMM"
    if len(s) >= 6 and not s.endswith("Z") and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]

    # "Z" -> "+0000"
    if s.endswith("Z"):
        s = s[:-1] + "+0000"

    # Add UTC if tz is missing (assume UTC)
    if len(s) == 14:  # YYYYMMDDHHMMSS
        s += "+0000"

    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def open_xml_stream(resp: requests.Response, url: str):
    """Return a file-like stream; transparently ungzip if needed."""
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    gz = url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct
    return gzip.GzipFile(fileobj=resp.raw) if gz else resp.raw

def pick_lang(elems: List[ET.Element], prefer: List[str]) -> str:
    """Pick element text by lang. STRICTLY from <title> (or <desc>) elements passed in."""
    best = ""
    # First pass: preferred langs
    for lang in prefer:
        for e in elems:
            if localname(e.tag) not in ("title", "desc"):
                continue
            elang = (e.attrib.get("lang") or "").strip().lower()
            if elang == lang:
                txt = "".join(e.itertext()).strip()
                if txt:
                    return html.unescape(txt)
    # Second pass: any non-empty
    for e in elems:
        if localname(e.tag) not in ("title", "desc"):
            continue
        txt = "".join(e.itertext()).strip()
        if txt:
            return html.unescape(txt)
    return best  # empty string

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
                    sleep_s = attempt * 0.6
                    log.warning("Retry %d/%d for %s (%d rows) in %.1fs: %s",
                                attempt, MAX_RETRIES, table, len(batch), sleep_s, msg)
                    time.sleep(sleep_s)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    sleep_s = attempt * 0.6
                    log.warning("Retry %d/%d for %s (%d rows) in %.1fs (unexpected): %s",
                                attempt, MAX_RETRIES, table, len(batch), sleep_s, e)
                    time.sleep(sleep_s)
    log.info("Upserted %d rows into %s.", total, table)

# ----------------------- Core ingest ------------------

def compute_window_utc() -> Tuple[datetime, datetime, datetime]:
    """
    Return (now_local, now_utc, horizon_utc).
    - now_local in WINDOW_TZ (DST aware)
    - now_utc, horizon_utc in UTC
    """
    local_tz = ZoneInfo(WINDOW_TZ)
    now_local = datetime.now(local_tz)  # DST-correct local time
    horizon_local = now_local + timedelta(hours=WINDOW_HOURS)
    now_utc = now_local.astimezone(timezone.utc)
    horizon_utc = horizon_local.astimezone(timezone.utc)
    return now_local, now_utc, horizon_utc

def parse_and_collect(url: str,
                      keep_window: bool,
                      now_utc: datetime,
                      horizon_utc: datetime) -> Tuple[Dict[str, dict], List[dict], int, float]:
    """Stream-parse a single XML/XML.GZ and collect channels/programs."""
    channels: Dict[str, dict] = {}
    programs: List[dict] = []
    empty_title_count = 0
    total_prog = 0

    log.info("Fetching EPG: %s", url)
    try:
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status_code == 404:
                log.warning("EPG URL 404: %s (skipping)", url)
                return channels, programs, total_prog, 0.0
            resp.raise_for_status()
            stream = open_xml_stream(resp, url)
            context = ET.iterparse(stream, events=("start", "end"))
            _, root = next(context)

            # Simple sampler for debugging
            sample_prog: List[dict] = []

            for ev, el in context:
                if ev != "end":
                    continue
                tag = localname(el.tag)

                if tag == "channel":
                    ch_id = el.get("id") or ""
                    if ch_id and (ch_id not in channels):
                        # Only keep the first non-empty display-name we see
                        display_txt = ""
                        for child in list(el):
                            if localname(child.tag) == "display-name":
                                display_txt = "".join(child.itertext()).strip()
                                display_txt = html.unescape(display_txt)
                                if display_txt:
                                    break
                        channels[ch_id] = {
                            "id": ch_id,
                            "display_name": display_txt or ch_id,
                            "icon_url": None
                        }
                    el.clear()
                    continue

                if tag == "programme":
                    total_prog += 1

                    ch_id = el.get("channel") or ""
                    s = parse_xmltv_datetime(el.get("start"))
                    e = parse_xmltv_datetime(el.get("stop"))
                    if not (ch_id and s and e):
                        el.clear(); continue

                    # Window filter (in UTC) if requested
                    if keep_window:
                        # keep if overlaps [now, horizon]
                        if not (s <= horizon_utc and e >= now_utc):
                            el.clear(); continue

                    # Title STRICTLY from <title>; no sub-title fallback
                    titles = [c for c in list(el) if localname(c.tag) == "title"]
                    descs  = [c for c in list(el) if localname(c.tag) == "desc"]

                    title = pick_lang(titles, PREFER_LANGS).strip()
                    desc  = pick_lang(descs,  PREFER_LANGS).strip()

                    if not title:
                        empty_title_count += 1
                        if SKIP_EMPTY_TITLES:
                            el.clear(); continue

                    # Program ID uses UTC timestamps to avoid tz ambiguity
                    pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                    programs.append({
                        "id": pid,
                        "channel_id": ch_id,
                        "start_time": s.isoformat(),  # UTC
                        "end_time": e.isoformat(),    # UTC
                        "title": title or None,
                        "description": desc or None,
                    })

                    # Debug-sample a few programmes
                    if len(sample_prog) < DEBUG_SAMPLE_PROGRAMS:
                        sample_prog.append({
                            "channel_id": ch_id, "start": s.isoformat(),
                            "title": title, "desc_len": len(desc or "")
                        })

                    el.clear()
                    if (len(programs) % 8000) == 0:
                        root.clear()
                    continue

                el.clear()

            # Log samples
            if channels:
                some_ch = list(channels.items())[:DEBUG_SAMPLE_CHANNELS]
                log.info("SAMPLE channels (%d):", len(some_ch))
                for cid, row in some_ch:
                    log.info("  id=%s name='%s'", cid, row["display_name"])
            if sample_prog:
                log.info("SAMPLE programmes (%d):", len(sample_prog))
                for p in sample_prog:
                    log.info("  ch=%s start=%s title=%r desc_len=%d",
                             p["channel_id"], p["start"], p["title"], p["desc_len"])

            titled_ratio = 0.0
            if total_prog:
                titled_ratio = (total_prog - empty_title_count) / float(total_prog)
            log.info("Parsed %s: channels(seen)=%d, programs_found=%d, kept=%d, titled_ratio=%.3f",
                     url, len(channels), total_prog, len(programs), titled_ratio)

            return channels, programs, total_prog, titled_ratio

    except requests.exceptions.RequestException as e:
        log.error("HTTP error for %s: %s", url, e)
    except ET.ParseError as e:
        log.error("XML parse error for %s: %s", url, e)
    except Exception as e:
        log.exception("Unexpected error for %s: %s", url, e)

    return channels, programs, total_prog, 0.0

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
    keep_window = WINDOW_HOURS > 0
    now_local, now_utc, horizon_utc = compute_window_utc()
    if keep_window:
        log.info("Windowing: ON (local=%s) -> UTC window: %s -> %s",
                 now_local.tzinfo, now_utc.isoformat(), horizon_utc.isoformat())
    else:
        log.info("Windowing: OFF")

    # Aggregate across all feeds
    channels_all: Dict[str, dict] = {}
    programs_all: Dict[str, dict] = {}  # id -> row

    for url in urls:
        chs, progs, _total, _ratio = parse_and_collect(url, keep_window, now_utc, horizon_utc)
        for cid, crow in chs.items():
            if cid not in channels_all:
                channels_all[cid] = crow
        for row in progs:
            pid = row["id"]
            # If duplicate pid appears, prefer the one with a non-empty title or longer description
            keep = programs_all.get(pid)
            if keep is None:
                programs_all[pid] = row
            else:
                t_old = (keep.get("title") or "").strip()
                t_new = (row.get("title") or "").strip()
                d_old = keep.get("description") or ""
                d_new = row.get("description") or ""
                replace = False
                if not t_old and t_new:
                    replace = True
                elif len(d_new) > len(d_old):
                    replace = True
                if replace:
                    programs_all[pid] = row

    # Upsert channels
    if channels_all:
        ch_rows = list(channels_all.values())
        log.info("Upserting %d channels …", len(ch_rows))
        upsert_with_retry(sb, "channels", ch_rows, conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels collected.")

    # Upsert programs
    prog_rows = list(programs_all.values())
    prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
    log.info("Programs to upsert (deduped): %d", len(prog_rows))
    if prog_rows:
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programs parsed for upsert.")

    # Sanity check (optional)
    try:
        if keep_window:
            res = sb.table("programs").select("id", count="exact")\
                .gte("end_time", now_utc.isoformat())\
                .lte("start_time", horizon_utc.isoformat())\
                .execute()
        else:
            res = sb.table("programs").select("id", count="exact").execute()
        total = getattr(res, "count", None) or 0
        # check how many have titles/descriptions
        with_title = sb.table("programs").select("id")\
            .not_.is_("title", "null").execute()
        with_desc = sb.table("programs").select("id")\
            .not_.is_("description", "null").execute()
        log.info("DB sanity: total=%d, with_title=%d, with_desc=%d",
                 total, len(getattr(with_title, "data", [])), len(getattr(with_desc, "data", [])))
    except Exception as e:
        log.warning("Sanity check failed: %s", e)

    # Clean up older than 12h (computed in local tz with DST, then UTC)
    try:
        cutoff_local = now_local - timedelta(hours=WINDOW_HOURS)
        cutoff_utc = cutoff_local.astimezone(timezone.utc)
        sb.table("programs").delete().lt("end_time", cutoff_utc.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s (UTC)", cutoff_utc.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # Refresh MV
    refresh_next_12h_mv(sb)

# ----------------------- Entrypoint -------------------

def main() -> int:
    log.info("Open-EPG ingest (PR/XML). WINDOW_HOURS=%d, WINDOW_TZ=%s, SKIP_EMPTY_TITLES=%s",
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
