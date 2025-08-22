#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, time, logging, itertools, gzip, io, re
from typing import Dict, List, Iterable, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from xml.etree import ElementTree as ET

from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Logging -----------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-epg")

# ----------------------- Config ------------------------

# Default to Open-EPG Puerto Rico XML (non-gz). You can extend this list.
DEFAULT_EPG_URLS = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]

# You can override via env (comma-separated)
_raw_urls = os.environ.get("OPEN_EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_EPG_URLS)

# Windowing OFF by default for your PR test (set to 0 = keep all)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "0"))

# Whether to skip programmes with empty titles before upsert
SKIP_EMPTY_TITLES = os.environ.get("SKIP_EMPTY_TITLES", "0") in ("1", "true", "True")

# Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# Batching
BATCH_CHANNELS = int(os.environ.get("BATCH_CHANNELS", "2000"))
BATCH_PROGRAMS = int(os.environ.get("BATCH_PROGRAMS", "1000"))

# Refresh MV function name (you created a 12h version earlier)
REFRESH_FUNC = os.environ.get("REFRESH_FUNC", "refresh_programs_next_12h")

MAX_RETRIES = 4
REQUEST_TIMEOUT = (10, 180)  # (connect, read)

# Debug helpers (optional) – can be set via workflow env
TEST_LOOKUP_CHANNEL = os.environ.get("TEST_LOOKUP_CHANNEL")  # e.g. 3ABN LATINO WTPM DT3 PUERTO RICO.pr
TEST_LOOKUP_START   = os.environ.get("TEST_LOOKUP_START")    # e.g. 20250822223000 +0000

EMPTY_SAMPLES_TO_LOG  = int(os.environ.get("EMPTY_SAMPLES_TO_LOG", "6"))
FILLED_SAMPLES_TO_LOG = int(os.environ.get("FILLED_SAMPLES_TO_LOG", "6"))

# ----------------------- Helpers -----------------------

def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, size))
        if not block:
            return
        yield block

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    """Parse XMLTV datetime to UTC-aware datetime."""
    if not raw:
        return None
    s = raw.strip()

    # remove accidental space before tz (e.g. '...  +0200')
    if " " in s:
        a, b = s.rsplit(" ", 1)
        # if looks like a tz suffix, glue it
        if re.fullmatch(r"[+-]\d{4}", b):
            s = a + b

    if s.endswith("Z"):
        s = s[:-1] + "+0000"

    # normalize +HH:MM -> +HHMM
    if len(s) >= 6 and (s.endswith("Z") is False) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]

    # add UTC if no tz and looks like YYYYMMDDHHMMSS
    if len(s) == 14 and s.isdigit():
        s += "+0000"

    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def xml_inner_text(elem: ET.Element) -> str:
    """
    Robustly extract inner text from an XML element.
    1) Try itertext()
    2) Fallback: serialize element and strip tags
    3) Clean whitespace
    """
    if elem is None:
        return ""
    try:
        txt = "".join(elem.itertext()).strip()
        if txt:
            return txt
    except Exception:
        pass

    try:
        raw = ET.tostring(elem, encoding="unicode", method="xml")
        # remove outer tag, keep inner content
        # e.g., <title>Some <b>text</b></title> -> "Some text"
        # naive but effective for “Excel-like” XMLs
        raw = re.sub(r"<[^>]+>", "", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw
    except Exception:
        return ""

def first_child_by_names(elem: ET.Element, wanted: Tuple[str, ...]) -> Optional[ET.Element]:
    wl = {w.lower() for w in wanted}
    for child in list(elem):
        name = child.tag.rsplit("}", 1)[-1].lower()  # strip namespace if present
        if name in wl:
            return child
    return None

def pick_programme_text(prog: ET.Element) -> Tuple[str, str]:
    """
    Return (title, description) strictly from <programme> children.
    Prefer <title>, fall back to <sub-title> for title. Desc from <desc>.
    """
    title_el = first_child_by_names(prog, ("title", "sub-title"))
    desc_el  = first_child_by_names(prog, ("desc",))

    title = xml_inner_text(title_el) if title_el is not None else ""
    desc  = xml_inner_text(desc_el)  if desc_el  is not None else ""

    # Normalize empty as ""
    title = title.strip()
    desc  = desc.strip()

    return (title, desc)

# ----------------------- Supabase ----------------------

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
    for batch in chunked(rows, base_batch):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch)
                break
            except APIError as e:
                msg = str(e)
                if attempt == MAX_RETRIES:
                    log.error("Giving up on %s batch (%d): %s", table, len(batch), msg)
                else:
                    time.sleep(0.6 * attempt)
                    log.warning("Retry %d/%d for %s (%d rows): %s", attempt, MAX_RETRIES, table, len(batch), msg)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    time.sleep(0.6 * attempt)
                    log.warning("Retry %d/%d for %s (%d rows) (unexpected): %s", attempt, MAX_RETRIES, table, len(batch), e)
    log.info("Upserted %d rows into %s.", total, table)

def refresh_mv(sb: Client):
    try:
        log.info("Refreshing MV via RPC: %s …", REFRESH_FUNC)
        sb.rpc(REFRESH_FUNC).execute()
        log.info("✅ MV refreshed.")
    except Exception as e:
        log.warning("MV refresh failed: %s", e)

# ----------------------- Fetch & Parse -----------------

def fetch_stream(url: str) -> io.BufferedReader:
    log.info("Fetching EPG (XML only): %s", url)
    resp = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers={
        "User-Agent": "Mozilla/5.0 (EPG Ingest; +https://github.com/)",
        "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.open-epg.com/",
    })
    resp.raise_for_status()
    resp.raw.decode_content = True
    return resp.raw  # file-like

def parse_open_epg_urls(urls: List[str]):
    """
    Parse only <programme> entries for titles/descriptions.
    Ignore 'columns A–D' notionally (i.e., the <channel> listing isn’t used for text).
    Still record channels we see via programme@channel, and optionally map a display name from <channel>.
    """
    now_utc = datetime.now(timezone.utc)
    horizon = now_utc + timedelta(hours=WINDOW_HOURS) if WINDOW_HOURS > 0 else None

    channels: Dict[str, dict] = {}   # id -> {id, display_name, icon_url}
    programmes: Dict[str, dict] = {} # pid -> row

    # For diagnostics
    empty_samples = []
    filled_samples = []
    titled_count = 0
    total_prog = 0

    for url in urls:
        try:
            stream = fetch_stream(url)
            context = ET.iterparse(stream, events=("start", "end"))
            _, root = next(context)

            for ev, el in context:
                if ev != "end":
                    continue

                tag = el.tag.rsplit("}", 1)[-1].lower()

                # We ignore channels for text, but we can keep names/icons for the channels table
                if tag == "channel":
                    ch_id = el.get("id")
                    if ch_id and ch_id not in channels:
                        # get display-name
                        disp_el = first_child_by_names(el, ("display-name",))
                        disp = xml_inner_text(disp_el) if disp_el is not None else ch_id
                        icon_el = first_child_by_names(el, ("icon",))
                        icon_url = None
                        if icon_el is not None:
                            # icon src may be in any-attr named 'src'
                            for k, v in icon_el.attrib.items():
                                if k.rsplit("}", 1)[-1].lower() == "src" and v:
                                    icon_url = v.strip()
                                    break
                        channels[ch_id] = {"id": ch_id, "display_name": disp or ch_id, "icon_url": icon_url}
                    el.clear()
                    continue

                if tag != "programme":
                    el.clear()
                    continue

                total_prog += 1

                ch_id = el.get("channel") or ""
                start_raw = el.get("start")
                stop_raw  = el.get("stop")

                s = parse_xmltv_datetime(start_raw)
                e = parse_xmltv_datetime(stop_raw)
                if not (ch_id and s and e):
                    el.clear(); continue

                # Window filter (OFF if WINDOW_HOURS==0)
                if horizon is not None:
                    if not (s <= horizon and e >= now_utc):
                        el.clear(); continue

                title, desc = pick_programme_text(el)

                # diagnostics sampling
                if title or desc:
                    if len(filled_samples) < FILLED_SAMPLES_TO_LOG:
                        filled_samples.append((ch_id, s.isoformat(), title, len(desc)))
                    titled_count += 1
                else:
                    if len(empty_samples) < EMPTY_SAMPLES_TO_LOG:
                        empty_samples.append((ch_id, s.isoformat()))

                # Build channel if we’ve never seen it (ignore <channel> header, per your request)
                if ch_id not in channels:
                    channels[ch_id] = {"id": ch_id, "display_name": ch_id, "icon_url": None}

                if SKIP_EMPTY_TITLES and not title:
                    el.clear(); continue

                pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                programmes[pid] = {
                    "id": pid,
                    "channel_id": ch_id,
                    "start_time": s.isoformat(),
                    "end_time": e.isoformat(),
                    "title": title or None,         # send None for empty
                    "description": (desc or None),  # send None for empty
                }

                el.clear()
                # free memory periodically
                if (total_prog % 12000) == 0:
                    root.clear()

            log.info("Parsed %s: channels(seen)=%d, programs_found=%d, kept=%d, titled_ratio=%.3f",
                     url, len(channels), total_prog, len(programmes),
                     (titled_count / total_prog if total_prog else 0.0))

        except Exception as e:
            log.exception("Error parsing %s: %s", url, e)

    # ---- diagnostics
    if filled_samples:
        log.info("SAMPLE non-empty programmes (%d):", len(filled_samples))
        for ch, st, ti, dl in filled_samples:
            log.info("  ch=%s start=%s title=%r desc_len=%d", ch, st, ti, dl)
    if empty_samples:
        log.info("SAMPLE EMPTY-looking programmes (%d):", len(empty_samples))
        for ch, st in empty_samples:
            log.info("  ch=%s start=%s", ch, st)

    return channels, programmes

# ----------------------- Main flow ---------------------

def main() -> int:
    log.info("Open-EPG ingest (PR, XML only). WINDOW_HOURS=%d, ENFORCE_LIVE=False, SKIP_EMPTY_TITLES=%s",
             WINDOW_HOURS, SKIP_EMPTY_TITLES)

    sb = init_supabase()

    if WINDOW_HOURS > 0:
        now_utc = datetime.now(timezone.utc)
        log.info("Windowing: ON  (%s -> %s UTC)", now_utc.isoformat(), (now_utc + timedelta(hours=WINDOW_HOURS)).isoformat())
    else:
        log.info("Windowing: OFF")

    # ---- parse open-epg
    channels, programmes = parse_open_epg_urls(EPG_URLS)

    # Optional: point test lookup
    if TEST_LOOKUP_CHANNEL and TEST_LOOKUP_START:
        # Try to find in parsed programmes by re-computing the pid pattern
        # TEST_LOOKUP_START is raw "YYYYMMDDHHMMSS +0000"
        sdt = parse_xmltv_datetime(TEST_LOOKUP_START)
        if sdt:
            found = False
            # We don't know stop, so scan all progs for this channel + same start
            for pid, row in programmes.items():
                if row["channel_id"] == TEST_LOOKUP_CHANNEL and row["start_time"] == sdt.isoformat():
                    log.info("TEST LOOKUP: %s @ %s -> title=%r, desc_len=%d",
                             TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START, row["title"], len(row.get("description") or ""))
                    found = True
                    break
            if not found:
                log.info("TEST LOOKUP: %s @ %s -> NOT FOUND in parsed set",
                         TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START)
        else:
            log.info("TEST LOOKUP: could not parse TEST_LOOKUP_START=%r", TEST_LOOKUP_START)

    # ---- upsert channels
    if channels:
        ch_rows = list(channels.values())
        log.info("Upserting %d channels …", len(ch_rows))
        upsert_with_retry(sb, "channels", ch_rows, conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels parsed.")

    # ---- upsert programs
    prog_rows = list(programmes.values())
    log.info("Programs to upsert (deduped): %d", len(prog_rows))
    prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))

    if prog_rows:
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programmes parsed (all filtered or parsing failed).")

    # ---- simple sanity counts in DB (optional & lightweight)
    try:
        # Count total programs
        res_total = sb.table("programs").select("id", count="exact").execute()
        db_total = getattr(res_total, "count", 0) or 0
        # Count non-null titles
        res_titled = sb.table("programs").select("id", count="exact").not_.is_("title", "null").execute()
        db_titled = getattr(res_titled, "count", 0) or 0
        # Count non-null descriptions
        res_desc = sb.table("programs").select("id", count="exact").not_.is_("description", "null").execute()
        db_desced = getattr(res_desc, "count", 0) or 0

        log.info("DB sanity: total=%d, with_title=%d, with_desc=%d", db_total, db_titled, db_desced)

        # Sample a few rows with titles to prove they landed
        if db_titled:
            sample = sb.table("programs")\
                .select("channel_id,start_time,title,description")\
                .not_.is_("title","null")\
                .limit(5).execute()
            items = getattr(sample, "data", []) or []
            if items:
                log.info("DB sample with titles (%d):", len(items))
                for it in items:
                    log.info("  ch=%s start=%s title=%r desc_len=%d",
                             it.get("channel_id"), it.get("start_time"),
                             it.get("title"), len((it.get("description") or "")))
    except Exception as e:
        log.warning("DB sanity check failed: %s", e)

    # refresh MV (12h)
    refresh_mv(sb)

    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))
    return 0

# ----------------------- Entrypoint --------------------

if __name__ == "__main__":
    sys.exit(main())
