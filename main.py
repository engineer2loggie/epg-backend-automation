#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, gzip, time, logging, itertools, random, json
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Set

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

DEFAULT_EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
]

REQUEST_TIMEOUT = (10, 180)
BATCH_CHANNELS = 2000
BATCH_PROGRAMS = 1000
MAX_RETRIES = 4

# ### CHANGED: 12h horizon by default
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))

# Refresh MV after ingest?
REFRESH_MV = os.environ.get("REFRESH_MV", "1") not in ("0","false","False","")

# ### NEW: only keep live streams & restrict countries
FILTER_LIVE = os.environ.get("FILTER_LIVE", "1") not in ("0","false","False","")
ALLOWED_COUNTRIES = os.environ.get(
    "ALLOWED_COUNTRIES",
    "PR,US,MX,ES,DE,CA,IT,GB,IE,CO,AU"
).replace(" ", "").split(",")

# iptv-org sources
IPTV_CHANNELS_URL = os.environ.get(
    "IPTV_CHANNELS_URL",
    "https://iptv-org.github.io/api/channels.json"
)
IPTV_STREAMS_URL = os.environ.get(
    "IPTV_STREAMS_URL",
    "https://iptv-org.github.io/api/streams.json"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_EPG_URLS)

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

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    s = raw.strip()
    if " " in s:
        a, b = s.rsplit(" ", 1)
        s = a + b
    if len(s) >= 6 and (not s.endswith("Z")) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]
    if s.endswith("Z"):
        s = s[:-1] + "+0000"
    if len(s) == 14:
        s += "+0000"
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def open_xml_stream(resp: requests.Response, url: str):
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    gz = url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct
    return gzip.GzipFile(fileobj=resp.raw) if gz else resp.raw

def localname(tag: str) -> str:
    if not tag:
        return tag
    if tag[0] == '{':
        return tag.split('}', 1)[1]
    return tag

def text_from(elem: ET.Element) -> str:
    return ''.join(elem.itertext()).strip() if elem is not None else ''

def first_text_by_names(elem: ET.Element, *names: str) -> str:
    wanted = {n.lower() for n in names}
    for child in list(elem):
        if localname(child.tag).lower() in wanted:
            txt = text_from(child)
            if txt:
                return txt
    return ''

def find_child(elem: ET.Element, name: str) -> Optional[ET.Element]:
    lname = name.lower()
    for child in list(elem):
        if localname(child.tag).lower() == lname:
            return child
    return None

def icon_src(elem: ET.Element) -> Optional[str]:
    ic = find_child(elem, 'icon')
    if ic is None:
        return None
    for k, v in ic.attrib.items():
        if localname(k).lower() == 'src' and v:
            return v.strip()
    return None

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
                    kd = (r.get("description") or "")
                    kd0 = (keep.get("description") or "")
                    kt = (r.get("title") or "")
                    kt0 = (keep.get("title") or "")
                    replace = False
                    if kt0.strip() == "No Title" and kt.strip() != "No Title":
                        replace = True
                    elif len(kd) > len(kd0):
                        replace = True
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
                need_split = ("21000" in msg or
                              "duplicate key value violates" in msg or
                              "500" in msg or
                              "413" in msg or
                              "Payload" in msg)
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
                                attempt, MAX_RETRIES, table, len(batch), e)
                    time.sleep(sleep_s)
    log.info("Upserted %d rows into %s.", total, table)

def refresh_next_12h_mv(sb: Client) -> None:
    if not REFRESH_MV:
        log.info("Skipping materialized view refresh (REFRESH_MV disabled).")
        return
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Refreshing materialized view: programs_next_12h …")
            sb.rpc("refresh_programs_next_12h").execute()
            log.info("✅ Materialized view refreshed.")
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.error("❌ Failed to refresh MV after %d attempts: %s", attempt, e)
                return
            sleep_s = attempt * rand_jitter()
            log.warning("Retry %d/%d refreshing MV in %.2fs: %s", attempt, MAX_RETRIES, sleep_s, e)
            time.sleep(sleep_s)

# -------------------- iptv-org helpers ----------------

def fetch_json(url: str) -> list:
    log.info("Fetching: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def build_live_channel_set() -> Set[str]:
    """
    Returns a set of channel IDs (matching XMLTV channel IDs)
    that are both (a) in allowed countries and (b) currently live.
    """
    if not FILTER_LIVE:
        log.info("FILTER_LIVE disabled; accepting all channels.")
        return set()

    channels = fetch_json(IPTV_CHANNELS_URL)  # list of dicts
    streams  = fetch_json(IPTV_STREAMS_URL)   # list of dicts

    # Map channel id -> country
    id_to_country: Dict[str, Optional[str]] = {}
    for ch in channels:
        cid = ch.get("id")
        ctry = (ch.get("country") or "").upper() or None
        if cid:
            id_to_country[cid] = ctry

    allowed = set([c.upper() for c in ALLOWED_COUNTRIES])
    live_ids: Set[str] = set()

    count_live = 0
    for st in streams:
        if st.get("status") != "online":
            continue
        cid = st.get("channel")
        if not cid:
            continue
        ctry = id_to_country.get(cid)
        if ctry and ctry in allowed:
            live_ids.add(cid)
            count_live += 1

    log.info("Live channels in allowed countries: %d", len(live_ids))
    return live_ids

# ----------------------- Core ingest ------------------

def fetch_and_process_epg(sb: Client, urls: List[str]):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
    log.info("Window: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())

    # ### NEW: restrict to live channels in allowed countries
    live_channels = build_live_channel_set()

    channels: Dict[str, dict] = {}  # id -> row
    programs: Dict[str, dict] = {}  # id -> row

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start", "end"))
                _, root = next(context)

                c_new = 0
                p_seen = 0
                p_kept = 0

                for ev, el in context:
                    if ev != "end":
                        continue

                    tag = localname(el.tag)

                    if tag == "channel":
                        ch_id = el.get("id")
                        if ch_id:
                            name = first_text_by_names(el, "display-name") or ch_id
                            icon = icon_src(el)
                            # If filtering by live, only keep live channels
                            if live_channels and ch_id not in live_channels:
                                el.clear(); continue
                            if ch_id not in channels:
                                channels[ch_id] = {
                                    "id": ch_id,
                                    "display_name": name,
                                    "icon_url": icon
                                }
                                c_new += 1
                        el.clear()
                        continue

                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel")
                        s = parse_xmltv_datetime(el.get("start"))
                        e = parse_xmltv_datetime(el.get("stop"))
                        if not (ch_id and s and e):
                            el.clear(); continue

                        # Must be within [now, now+WINDOW_HOURS]
                        if not (s <= horizon_utc and e >= now_utc):
                            el.clear(); continue

                        # Must be live channel (if filtering)
                        if live_channels and ch_id not in live_channels:
                            el.clear(); continue

                        title = (first_text_by_names(el, "title", "sub-title") or "No Title").strip()
                        desc  = first_text_by_names(el, "desc").strip() or None

                        pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                        row = {
                            "id": pid,
                            "channel_id": ch_id,
                            "start_time": s.isoformat(),
                            "end_time": e.isoformat(),
                            "title": title,
                            "description": desc
                        }

                        prev = programs.get(pid)
                        if prev is None:
                            programs[pid] = row
                            p_kept += 1
                        else:
                            # prefer rows with real title and longer description
                            prev_t = (prev.get("title") or "").strip()
                            cand_t = (row.get("title") or "").strip()
                            prev_d = (prev.get("description") or "") or ""
                            cand_d = (row.get("description") or "") or ""
                            if (prev_t == "No Title" and cand_t != "No Title") or (len(cand_d) > len(prev_d)):
                                programs[pid] = row

                        el.clear()
                        if (p_kept % 8000) == 0:
                            root.clear()
                        continue

                    el.clear()

                log.info(
                    "Parsed file done: channels(new)=%d, programs_found=%d, programs_kept_%dh=%d",
                    c_new, p_seen, WINDOW_HOURS, p_kept
                )

        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

    # Ensure all program.channel_id exist in channels (should normally be true)
    referenced = {p["channel_id"] for p in programs.values()}
    missing = referenced.difference(channels.keys())
    if missing:
        log.warning("Creating %d placeholder channels for missing IDs.", len(missing))
        for ch in missing:
            channels[ch] = {"id": ch, "display_name": ch, "icon_url": None}

    # ---- Upserts ----
    if channels:
        upsert_with_retry(sb, "channels", list(channels.values()), conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels to upsert.")

    prog_rows = list(programs.values())
    log.info("Programs to upsert (deduped): %d", len(prog_rows))

    if prog_rows:
        prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programs kept in %d-hour window. (Check time parsing/window/live filter)", WINDOW_HOURS)

    # Cleanup: remove anything older than now-12h (keeps table tiny)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # Refresh MV (12h)
    refresh_next_12h_mv(sb)

    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))

# ----------------------- Entrypoint -------------------

def main() -> int:
    log.info("EPG ingest starting. URLs: %s", ", ".join(EPG_URLS))
    log.info("FILTER_LIVE=%s, ALLOWED_COUNTRIES=%s, WINDOW_HOURS=%d",
             FILTER_LIVE, ",".join(ALLOWED_COUNTRIES), WINDOW_HOURS)
    sb = init_supabase()

    # Optional: hard wipe each run to guarantee small DB
    if os.environ.get("TRUNCATE_ON_START", "0") in ("1","true","True"):
        try:
            sb.rpc("exec_sql", {"sql": "TRUNCATE TABLE public.programs RESTART IDENTITY; TRUNCATE TABLE public.channels RESTART IDENTITY;"}).execute()
            log.info("Truncated tables at start (TRUNCATE_ON_START=1).")
        except Exception:
            # If you don’t have an exec_sql RPC, just skip silently.
            pass

    t0 = time.time()
    fetch_and_process_epg(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
