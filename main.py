#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, gzip, time, logging, itertools, random, io, unicodedata
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Set, Tuple

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

# Default 12h window
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))

# Countries you support in the app (ISO 3166-1 alpha-2, comma-separated)
ALLOWED_COUNTRIES_ENV = os.environ.get("ALLOWED_COUNTRIES", "PR,US,MX,ES,DE,CA,IT,GB,IE,CO,AU")
ALLOWED_COUNTRIES: Set[str] = {c.strip().upper() for c in ALLOWED_COUNTRIES_ENV.split(",") if c.strip()}

# Filter to only channels that are "live" per iptv-org (i.e., have at least one stream)
FILTER_LIVE = os.environ.get("FILTER_LIVE", "1") not in ("0", "false", "False", "")

# Debug sampling
DEBUG_SAMPLE = int(os.environ.get("DEBUG_SAMPLE", "10"))  # number of examples to log per category
DEBUG_CHANNELS = {c.strip() for c in os.environ.get("DEBUG_CHANNELS", "").split(",") if c.strip()}  # optional, exact EPG channel ids

# EPG URLs: if env not provided, use epg.pw per-country (gzipped)
DEFAULT_EPG_URLS = [
    "https://epg.pw/xmltv/epg_US.xml.gz",
    "https://epg.pw/xmltv/epg_PR.xml.gz",
    "https://epg.pw/xmltv/epg_MX.xml.gz",
    "https://epg.pw/xmltv/epg_ES.xml.gz",
    "https://epg.pw/xmltv/epg_DE.xml.gz",
    "https://epg.pw/xmltv/epg_CA.xml.gz",
    "https://epg.pw/xmltv/epg_IT.xml.gz",
    "https://epg.pw/xmltv/epg_GB.xml.gz",
    "https://epg.pw/xmltv/epg_IE.xml.gz",
    "https://epg.pw/xmltv/epg_CO.xml.gz",
    "https://epg.pw/xmltv/epg_AU.xml.gz",
]

_raw_urls = os.environ.get("EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] or list(DEFAULT_EPG_URLS)

# iptv-org API endpoints
IPTVORG_CHANNELS_URL = os.environ.get("IPTVORG_CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
IPTVORG_STREAMS_URL  = os.environ.get("IPTVORG_STREAMS_URL",  "https://iptv-org.github.io/api/streams.json")

REQUEST_TIMEOUT = (10, 180)  # (connect, read)
BATCH_CHANNELS = int(os.environ.get("BATCH_CHANNELS", "2000"))
BATCH_PROGRAMS = int(os.environ.get("BATCH_PROGRAMS", "1000"))
MAX_RETRIES = 4

# Refresh MV after ingest
REFRESH_MV = os.environ.get("REFRESH_MV", "1") not in ("0", "false", "False", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

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

def open_xml_stream(resp: requests.Response, url: str):
    """Return a file-like stream; transparently ungzip if needed."""
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    gz = url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct
    return gzip.GzipFile(fileobj=resp.raw) if gz else resp.raw

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    """Parse XMLTV datetime to UTC-aware datetime."""
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
    if len(s) == 14:  # YYYYMMDDHHMMSS
        s += "+0000"
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def localname(tag: str) -> str:
    if not tag:
        return tag
    if tag[0] == "{":
        return tag.split("}", 1)[1]
    return tag

def text_from(elem: Optional[ET.Element]) -> str:
    return ''.join(elem.itertext()).strip() if elem is not None else ''

def iter_children(elem: ET.Element, name: str):
    lname = name.lower()
    for child in list(elem):
        if localname(child.tag).lower() == lname:
            yield child

def first_text_by_names(elem: ET.Element, *names: str) -> str:
    wanted = {n.lower() for n in names}
    for child in list(elem):
        if localname(child.tag).lower() in wanted:
            txt = text_from(child)
            if txt:
                return txt
    return ''

def icon_src(elem: ET.Element) -> Optional[str]:
    for ic in iter_children(elem, "icon"):
        for k, v in ic.attrib.items():
            if localname(k).lower() == "src" and v:
                return v.strip()
    return None

def norm_name(s: str) -> str:
    """Normalize names for fuzzy equality: lowercase, strip, collapse spaces, remove accents."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = ' '.join(s.split())
    return s

JUNK_TITLES = {"", "title", "no title"}

def is_junky_title(s: str) -> bool:
    return norm_name(s) in JUNK_TITLES

# ----------------------- iptv-org live filter -----------------------

def fetch_json(url: str) -> list:
    log.info("Fetching JSON: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def build_live_name_whitelist(allowed_countries: Set[str]) -> Tuple[Set[str], Dict[str, dict]]:
    """
    Returns:
      live_names: set of normalized channel names that have at least one stream AND are in allowed countries
      channel_index: {channel_id: channel_record} for allowed countries
    """
    chans = fetch_json(IPTVORG_CHANNELS_URL)  # [{'id','name','country',...}]
    streams = fetch_json(IPTVORG_STREAMS_URL) # [{'channel','url',...}]
    # Build set of channel IDs that actually have streams (this is iptv-org's effective "live")
    have_streams: Set[str] = {s.get("channel") for s in streams if s.get("channel")}
    # Index channels
    channel_index: Dict[str, dict] = {}
    live_names: Set[str] = set()
    kept = 0
    for ch in chans:
        cid = ch.get("id")
        if not cid:
            continue
        country = (ch.get("country") or "").upper()
        if country not in allowed_countries:
            continue
        if cid not in have_streams:
            continue
        name = ch.get("name") or ""
        if name:
            live_names.add(norm_name(name))
        # also index by alt_names if present
        for alt in ch.get("alt_names") or []:
            if alt:
                live_names.add(norm_name(alt))
        channel_index[cid] = ch
        kept += 1
    log.info("iptv-org live filter: kept %d channels with streams in allowed countries (names in whitelist=%d)",
             kept, len(live_names))
    return live_names, channel_index

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
                    # prefer better title/longer description
                    kd = (r.get("description") or "")
                    kd0 = (keep.get("description") or "")
                    kt = (r.get("title") or "")
                    kt0 = (keep.get("title") or "")
                    replace = False
                    if is_junky_title(kt0) and not is_junky_title(kt):
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
                need_split = ("21000" in msg or "duplicate key value violates" in msg or
                              "500" in msg or "413" in msg or "Payload" in msg)
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

def refresh_next_mv(sb: Client) -> None:
    """Calls secure RPC to refresh the 12h MV; fallback to 24h if needed."""
    if not REFRESH_MV:
        log.info("Skipping materialized view refresh (REFRESH_MV disabled).")
        return
    funcs = ["refresh_programs_next_12h", "refresh_programs_next_24h"]
    for fn in funcs:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                log.info("Refreshing materialized view via RPC: %s …", fn)
                sb.rpc(fn).execute()
                log.info("✅ Materialized view refreshed.")
                return
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.warning("MV refresh via %s failed after %d attempts: %s", fn, attempt, e)
                    break
                sleep_s = attempt * rand_jitter()
                log.warning("Retry %d/%d refreshing MV (%s) in %.2fs: %s", attempt, MAX_RETRIES, fn, sleep_s, e)
                time.sleep(sleep_s)
    log.error("❌ MV refresh failed via both RPCs.")

# ----------------------- Core ingest ------------------

def fetch_and_process_epg(sb: Client, urls: List[str]):
    log.info("FILTER_LIVE=%s, ALLOWED_COUNTRIES=%s, WINDOW_HOURS=%d, DEBUG_SAMPLE=%d, DEBUG_CHANNELS=%s",
             bool(FILTER_LIVE), ",".join(sorted(ALLOWED_COUNTRIES)), WINDOW_HOURS, DEBUG_SAMPLE,
             ("(any)" if not DEBUG_CHANNELS else ",".join(sorted(DEBUG_CHANNELS))))
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
    log.info("Window: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())

    # Build iptv-org "live" name whitelist (channels that have at least one stream) within allowed countries
    live_name_whitelist: Set[str] = set()
    if FILTER_LIVE:
        try:
            live_name_whitelist, _ = build_live_name_whitelist(ALLOWED_COUNTRIES)
        except Exception as e:
            log.warning("Failed to build iptv-org live name whitelist (continuing without live filter): %s", e)
            FILTER = False
            live_name_whitelist = set()

    channels: Dict[str, dict] = {}  # EPG channel_id -> row
    allowed_epg_channels: Set[str] = set()
    programs: Dict[str, dict] = {}  # program_id -> row

    # Debug collectors
    dbg_kept: List[str] = []
    dbg_skipped_empty: List[str] = []
    dbg_skipped_notlive: List[str] = []

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status_code == 404:
                    log.warning("EPG URL 404: %s (skipping)", url)
                    continue
                resp.raise_for_status()
                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start", "end"))
                _, root = next(context)

                # temp map: EPG channel id -> normalized display names seen
                epg_id_to_names: Dict[str, Set[str]] = {}

                p_seen = 0
                p_kept = 0
                c_seen = 0
                c_kept = 0

                for ev, el in context:
                    if ev != "end":
                        continue

                    tag = localname(el.tag)

                    # --------- <channel> ----------
                    if tag == "channel":
                        c_seen += 1
                        ch_id = el.get("id") or ""
                        # collect all display-name values
                        names = set()
                        for dn in iter_children(el, "display-name"):
                            t = text_from(dn)
                            if t:
                                names.add(norm_name(t))
                        icon = icon_src(el)

                        # Decide if we keep this channel
                        keep = True
                        if FILTER_LIVE and live_name_whitelist:
                            keep = any(n in live_name_whitelist for n in names) if names else False

                        if keep:
                            channels[ch_id] = {
                                "id": ch_id,
                                "display_name": next(iter(names), ch_id) or ch_id,
                                "icon_url": icon
                            }
                            allowed_epg_channels.add(ch_id)
                            c_kept += 1
                        else:
                            # For debugging: sample a couple of not-live channels that got filtered out
                            if len(dbg_skipped_notlive) < DEBUG_SAMPLE:
                                dbg_skipped_notlive.append(f"EPG channel SKIPPED (not live-match): id={ch_id} names={sorted(list(names))[:3]}")
                        epg_id_to_names[ch_id] = names or {norm_name(ch_id)}
                        el.clear()
                        continue

                    # --------- <programme> ----------
                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel") or ""
                        if FILTER_LIVE and ch_id not in allowed_epg_channels:
                            # skip programmes for channels we didn't keep
                            if len(dbg_skipped_notlive) < DEBUG_SAMPLE and (not DEBUG_CHANNELS or ch_id in DEBUG_CHANNELS):
                                snippet = ET.tostring(el, encoding="unicode")
                                dbg_skipped_notlive.append(f"programme SKIPPED (channel not-live): ch={ch_id}\n  raw: {snippet[:300]}...")
                            el.clear()
                            continue

                        s = parse_xmltv_datetime(el.get("start"))
                        e = parse_xmltv_datetime(el.get("stop"))
                        if not (ch_id and s and e):
                            el.clear(); continue

                        # keep only items overlapping [now, now+WINDOW_HOURS]
                        if not (s <= horizon_utc and e >= now_utc):
                            el.clear(); continue

                        title = (first_text_by_names(el, "title", "sub-title") or "").strip()
                        desc  = (first_text_by_names(el, "desc") or "").strip()

                        # Drop empties/junk titles
                        if is_junky_title(title):
                            if len(dbg_skipped_empty) < DEBUG_SAMPLE and (not DEBUG_CHANNELS or ch_id in DEBUG_CHANNELS):
                                snippet = ET.tostring(el, encoding="unicode")
                                dbg_skipped_empty.append(
                                    f"programme SKIPPED (empty/junk title): ch={ch_id} start={s.isoformat()} stop={e.isoformat()}\n"
                                    f"  title={repr(title)} desc_len={len(desc)}\n  raw: {snippet[:300]}...")
                            el.clear(); continue

                        pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                        row = {
                            "id": pid,
                            "channel_id": ch_id,
                            "start_time": s.isoformat(),
                            "end_time": e.isoformat(),
                            "title": title,
                            "description": desc or None
                        }

                        prev = programs.get(pid)
                        if prev is None:
                            programs[pid] = row
                            p_kept += 1
                            if len(dbg_kept) < DEBUG_SAMPLE and (not DEBUG_CHANNELS or ch_id in DEBUG_CHANNELS):
                                snippet = ET.tostring(el, encoding="unicode")
                                dbg_kept.append(
                                    f"programme KEPT: ch={ch_id} start={s.isoformat()} stop={e.isoformat()}\n"
                                    f"  title={repr(title)} desc_len={len(desc)}\n  raw: {snippet[:300]}...")
                        else:
                            # prefer better title/longer description
                            prev_t = prev.get("title") or ""
                            cand_t = row.get("title") or ""
                            prev_d = prev.get("description") or ""
                            cand_d = row.get("description") or ""
                            replace = False
                            if is_junky_title(prev_t) and not is_junky_title(cand_t):
                                replace = True
                            elif len(cand_d) > len(prev_d):
                                replace = True
                            if replace:
                                programs[pid] = row

                        el.clear()
                        if (p_kept % 8000) == 0:
                            root.clear()
                        continue

                    el.clear()

                log.info("Parsed file done: channels(seen)=%d, channels(kept)=%d, programs_found=%d, programs_kept_%dh=%d",
                         c_seen, c_kept, p_seen, WINDOW_HOURS, p_kept)

        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

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
        log.warning("No programmes kept (check live filter / feeds / window).")

    # verify
    cnt = count_programs_in_window(sb, now_utc, horizon_utc)
    if cnt >= 0:
        log.info("✅ Supabase now has %d programs in the %dh window.", cnt, WINDOW_HOURS)
    else:
        log.info("⚠️ Skipped verification count due to error.")

    # cleanup: old programs
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # refresh MV (secure RPC)
    refresh_next_mv(sb)

    # Debug summary
    log.info("DEBUG kept samples (%d):", len(dbg_kept))
    for s in dbg_kept[:DEBUG_SAMPLE]:
        log.info(s)
    log.info("DEBUG skipped (empty/junk title) samples (%d):", len(dbg_skipped_empty))
    for s in dbg_skipped_empty[:DEBUG_SAMPLE]:
        log.info(s)
    log.info("DEBUG skipped (not-live) samples (%d):", len(dbg_skipped_notlive))
    for s in dbg_skipped_notlive[:DEBUG_SAMPLE]:
        log.info(s)

    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))

# ----------------------- Entrypoint -------------------

def main() -> int:
    log.info("EPG ingest starting. URLs (%d): %s",
             len(EPG_URLS), ", ".join(EPG_URLS) if EPG_URLS else "(none provided)")
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_epg(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
