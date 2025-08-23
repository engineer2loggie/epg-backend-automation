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

# ------------- Config -------------

# Use the *plain* XMLTV files (NOT .gz), e.g. Puerto Rico pilot:
DEFAULT_OPEN_EPG_URLS = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]

WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))
WINDOW_TZ    = os.environ.get("WINDOW_TZ", "America/Puerto_Rico")
PREFER_LANGS = [s.strip().lower() for s in os.environ.get("PREFER_LANGS", "es-pr,es,en").split(",") if s.strip()]

BATCH_CHANNELS  = 1000
BATCH_PROGRAMS  = 800
MAX_RETRIES     = 4
REQUEST_TIMEOUT = (10, 180)

DEBUG_SAMPLE = int(os.environ.get("DEBUG_SAMPLE", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("xmltv-open-epg")

SUPABASE_URL         = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("OPEN_EPG_URLS", "")
OPEN_EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_OPEN_EPG_URLS)

if WINDOW_HOURS <= 0:
    WINDOW_HOURS = 12

# ------------- Helpers -------------

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

def parse_epg_dt(raw: Optional[str]) -> Optional[datetime]:
    """
    Parse XMLTV timestamps like 'YYYYMMDDHHMMSS +0000' or 'YYYYMMDDHHMMSS+0000' or '...Z'
    Return tz-aware UTC datetime.
    """
    if not raw:
        return None
    s = raw.strip()
    if " " in s:
        a, b = s.rsplit(" ", 1)
        if (len(b) in (5, 6)) and (b[0] in "+-0123456789Z"):
            s = a + b
    if s.endswith("Z"):
        s = s[:-1] + "+0000"
    if len(s) == 14:  # missing tz
        s += "+0000"
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

        # De-dup by id (prefer non-empty title / longer description)
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
                    t0 = (keep.get("title") or "").strip()
                    t1 = (r.get("title") or "").strip()
                    d0 = keep.get("description") or ""
                    d1 = r.get("description") or ""
                    if (not t0 and t1) or (len(d1) > len(d0)):
                        dedup[k] = r
            batch = list(dedup.values())

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch)
                break
            except APIError as e:
                msg = str(e)
                need_split = ("duplicate key" in msg) or ("21000" in msg) or ("413" in msg) or ("Payload" in msg) or ("500" in msg)
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

# ------------- XMLTV parsing -------------

def pick_lang_text(elems: List[ET.Element], prefer_langs: List[str]) -> str:
    """Return text from the first element whose @lang matches preference (case-insensitive), else any non-empty."""
    # normalize: join all itertext
    def txt(el: ET.Element) -> str:
        return "".join(el.itertext()).strip() if el is not None else ""
    # first pass: preferred languages
    for pl in prefer_langs:
        for el in elems:
            if (el is not None) and (el.get("lang","").lower() == pl):
                t = txt(el)
                if t:
                    return t
    # second pass: any non-empty
    for el in elems:
        t = txt(el)
        if t:
            return t
    return ""

def parse_xmltv_urls(urls: List[str], window_start_utc: datetime, window_end_utc: datetime) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    """
    Parse XMLTV files and return (channels, programs) dicts.
    channels: {id -> {id, display_name, icon_url}}
    programs: {pid -> row}
    """
    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}

    for url in urls:
        log.info("Fetching EPG (XMLTV): %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                resp.raw.decode_content = True

                context = ET.iterparse(resp.raw, events=("start", "end"))
                _, root = next(context)

                seen_prog = kept_prog = titled = 0

                for ev, el in context:
                    if ev != "end":
                        continue
                    tag = localname(el.tag)

                    if tag == "channel":
                        cid = el.get("id") or ""
                        cid = cid.strip()
                        if cid:
                            name = ""
                            icon = None
                            for child in el:
                                ctag = localname(child.tag)
                                if ctag == "display-name" and not name:
                                    name = "".join(child.itertext()).strip()
                                elif ctag == "icon" and (icon is None):
                                    icon = child.get("src")
                            channels.setdefault(cid, {"id": cid, "display_name": name or cid, "icon_url": icon})
                        el.clear()

                    elif tag == "programme":
                        seen_prog += 1
                        ch = el.get("channel") or ""
                        st_raw = el.get("start")
                        en_raw = el.get("stop") or el.get("end")  # some feeds use stop

                        s = parse_epg_dt(st_raw)
                        e = parse_epg_dt(en_raw)
                        if not (ch and s and e):
                            el.clear()
                            continue

                        # Keep only rows overlapping the 12h window
                        if not (s <= window_end_utc and e >= window_start_utc):
                            el.clear()
                            continue

                        titles = [c for c in el if localname(c.tag) == "title"]
                        descs  = [c for c in el if localname(c.tag) == "desc"]

                        title = pick_lang_text(titles, PREFER_LANGS)   # STRICTLY from <title>
                        desc  = pick_lang_text(descs,  PREFER_LANGS)

                        if title:
                            titled += 1

                        pid = f"{ch}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                        programs[pid] = {
                            "id": pid,
                            "channel_id": ch,
                            "start_time": s.isoformat(),
                            "end_time": e.isoformat(),
                            "title": title or None,          # store NULL if empty
                            "description": desc or None
                        }
                        kept_prog += 1

                        el.clear()

                    # Periodic freeing
                    if (seen_prog % 5000) == 0:
                        root.clear()

                ratio = (titled / kept_prog) if kept_prog else 0.0
                log.info("Parsed %s: programs_found=%d, kept=%d, titled_ratio=%.3f",
                         url, seen_prog, kept_prog, ratio)

        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

    return channels, programs

# ------------- Core ingest -------------

def main() -> int:
    log.info("Open-EPG ingest (XMLTV). WINDOW_HOURS=%d, WINDOW_TZ=%s, SKIP_EMPTY_TITLES=False",
             WINDOW_HOURS, WINDOW_TZ)

    sb = init_supabase()

    # DST-aware window: local -> UTC
    try:
        loc_tz = ZoneInfo(WINDOW_TZ)
    except Exception:
        loc_tz = ZoneInfo("UTC")
    now_local = datetime.now(loc_tz)
    end_local = now_local + timedelta(hours=WINDOW_HOURS)
    window_start_utc = now_local.astimezone(timezone.utc)
    window_end_utc   = end_local.astimezone(timezone.utc)

    log.info("Windowing: ON (local tz=%s) -> UTC window: %s -> %s",
             WINDOW_TZ, window_start_utc.isoformat(), window_end_utc.isoformat())

    channels, programs = parse_xmltv_urls(OPEN_EPG_URLS, window_start_utc, window_end_utc)

    # Samples
    if programs:
        sample = list(programs.values())[:DEBUG_SAMPLE]
        log.info("SAMPLE programmes (%d):", len(sample))
        for r in sample:
            log.info("  ch=%s start=%s end=%s title=%r desc_len=%d",
                     r["channel_id"], r["start_time"], r["end_time"],
                     r.get("title") or "", len(r.get("description") or ""))

    # Upserts
    if channels:
        log.info("Upserting %d channels …", len(channels))
        upsert_with_retry(sb, "channels", list(channels.values()), conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels collected from XMLTV.")

    prog_rows = list(programs.values())
    log.info("Programs to upsert (deduped): %d", len(prog_rows))
    if prog_rows:
        prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programs parsed for upsert.")

    # Sanity in-window
    cnt = count_programs_in_window(sb, window_start_utc, window_end_utc)
    if cnt >= 0:
        log.info("✅ Supabase now has %d programs in the 12h window.", cnt)

    # Cleanup: delete anything older than 12h
    cutoff = window_start_utc - timedelta(hours=12)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s (UTC)", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # Refresh MV
    refresh_next_12h_mv(sb)

    log.info("Finished.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
