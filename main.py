#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, gzip, time, logging, itertools, random
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Set, Tuple

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# =====================================================
# Config
# =====================================================

# IMPORTANT: provide EPG_URLS via env as a comma-separated list of country feeds.
# Example:
#   EPG_URLS="https://.../epg_US.xml.gz,https://.../epg_CA.xml.gz,..."
_raw_urls = os.environ.get("EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()]

REQUEST_TIMEOUT = (10, 180)  # (connect, read)
BATCH_CHANNELS   = 2000
BATCH_PROGRAMS   = 1000
MAX_RETRIES      = 4

# Ingest horizon (hours)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))

# Live + country filtering
FILTER_LIVE  = os.environ.get("FILTER_LIVE", "1") not in ("0","false","False","")
STRICT_LIVE  = os.environ.get("STRICT_LIVE", "1") not in ("0","false","False","")  # require status=="online"
ALLOWED_COUNTRIES = os.environ.get(
    "ALLOWED_COUNTRIES",
    "PR,US,MX,ES,DE,CA,IT,GB,IE,CO,AU"
).replace(" ", "").split(",")

# iptv-org API (channels/streams)
IPTV_CHANNELS_URL = os.environ.get(
    "IPTV_CHANNELS_URL",
    "https://iptv-org.github.io/api/channels.json"
)
IPTV_STREAMS_URL = os.environ.get(
    "IPTV_STREAMS_URL",
    "https://iptv-org.github.io/api/streams.json"
)

# Debug sampler — log some <programme> raw XML
DEBUG_SAMPLE   = int(os.environ.get("DEBUG_SAMPLE", "8"))  # set 0 to disable
DEBUG_CHANNELS = [s.strip() for s in os.environ.get("DEBUG_CHANNELS", "").split(",") if s.strip()]

# Materialized view refresh RPC
REFRESH_MV      = os.environ.get("REFRESH_MV", "1") not in ("0","false","False","")
REFRESH_MV_FUNC = os.environ.get("REFRESH_MV_FUNC", "refresh_programs_next_12h")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg")

SUPABASE_URL         = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# =====================================================
# Helpers
# =====================================================

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
    """Parse XMLTV datetime --> timezone-aware UTC datetime."""
    if not raw:
        return None
    s = raw.strip()
    # remove possible extra space before tz (" +0000")
    if " " in s:
        a, b = s.rsplit(" ", 1)
        s = a + b
    # normalize +HH:MM -> +HHMM
    if len(s) >= 6 and (not s.endswith("Z")) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]
    # trailing Z -> +0000
    if s.endswith("Z"):
        s = s[:-1] + "+0000"
    # add tz if missing
    if len(s) == 14:
        s += "+0000"
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def open_xml_stream(resp: requests.Response, url: str):
    """Return file-like stream; ungzip when needed."""
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

def short_xml(elem: ET.Element, max_len: int = 3000) -> str:
    try:
        s = ET.tostring(elem, encoding="unicode")
    except Exception:
        s = "<failed to serialize>"
    return s if len(s) <= max_len else (s[:max_len] + "…(truncated)")

# =====================================================
# Title/Description selection
# =====================================================

JUNK_VALUES = {"", "title", "no title", "no information", "n/a", "unknown", "sin información"}

def all_text_nodes(elem: ET.Element, local: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    lname = local.lower()
    for child in list(elem):
        if localname(child.tag).lower() == lname:
            lang = (child.attrib.get("lang") or child.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") or "")
            out.append((lang.lower(), text_from(child)))
    return out

def pick_best_text(pairs: List[Tuple[str, str]], preferred_langs=("en","es","de","it","fr","pt")) -> Optional[str]:
    cleaned = [(lang, (txt or "").strip()) for (lang, txt) in pairs if txt and (txt.strip().lower() not in JUNK_VALUES)]
    if not cleaned:
        return None
    ranked = sorted(
        cleaned,
        key=lambda lt: (
            (preferred_langs.index(lt[0]) if lt[0] in preferred_langs else 999),
            -len(lt[1])
        )
    )
    return ranked[0][1] if ranked else None

# =====================================================
# Supabase
# =====================================================

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
                    kd  = (r.get("description") or "")
                    kd0 = (keep.get("description") or "")
                    kt  = (r.get("title") or "")
                    kt0 = (keep.get("title") or "")
                    replace = False
                    if kt0.strip().lower() in ("no title", "title", "") and kt.strip().lower() not in ("no title", "title", ""):
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
                need_split = ("21000" in msg or "duplicate key value violates" in msg or "500" in msg or "413" in msg or "Payload" in msg)
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
                    log.warning("Retry %d/%d for %s (%d rows) in %.2fs: %s", attempt, MAX_RETRIES, table, len(batch), sleep_s, msg)
                    time.sleep(sleep_s)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    sleep_s = attempt * rand_jitter()
                    log.warning("Retry %d/%d for %s (%d rows) in %.2fs (unexpected): %s", attempt, MAX_RETRIES, table, len(batch), e)
                    time.sleep(sleep_s)
    log.info("Upserted %d rows into %s.", total, table)

def refresh_mv(sb: Client) -> None:
    if not REFRESH_MV:
        log.info("Skipping MV refresh (REFRESH_MV disabled).")
        return
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Refreshing materialized view via RPC: %s …", REFRESH_MV_FUNC)
            sb.rpc(REFRESH_MV_FUNC).execute()
            log.info("✅ Materialized view refreshed.")
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.error("❌ Failed to refresh MV after %d attempts: %s", attempt, e)
                return
            sleep_s = attempt * rand_jitter()
            log.warning("Retry %d/%d refreshing MV in %.2fs: %s", attempt, MAX_RETRIES, sleep_s, e)
            time.sleep(sleep_s)

# =====================================================
# iptv-org helpers (strict LIVE + country filtering)
# =====================================================

def fetch_json(url: str) -> list:
    log.info("Fetching JSON: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def build_live_channel_set() -> Set[str]:
    """
    Returns channel IDs (XMLTV ids) that are BOTH:
      - in ALLOWED_COUNTRIES (from channels.json), and
      - currently LIVE per streams.json (status == "online") when FILTER_LIVE is on.

    STRICT_LIVE=True (default) => require status == "online"
    STRICT_LIVE=False          => allow any stream entry (less strict)
    """
    if not FILTER_LIVE:
        log.info("FILTER_LIVE disabled; accepting all channels from EPG.")
        return set()

    channels = fetch_json(IPTV_CHANNELS_URL)  # [{id, country, ...}]
    streams  = fetch_json(IPTV_STREAMS_URL)   # [{channel, status?, ...}]

    # country map
    id_to_country: Dict[str, Optional[str]] = {}
    for ch in channels:
        cid = ch.get("id")
        ctry = (ch.get("country") or "").upper() or None
        if cid:
            id_to_country[cid] = ctry

    allowed = {c.upper() for c in ALLOWED_COUNTRIES}

    # detect if status exists in data
    has_status = any(isinstance(s, dict) and "status" in s for s in streams)
    if STRICT_LIVE and not has_status:
        log.warning("STRICT_LIVE is ON but streams.json lacks 'status'; result will be EMPTY.")
        return set()

    live_ids: Set[str] = set()
    for st in streams:
        cid = st.get("channel")
        if not cid:
            continue
        ctry = id_to_country.get(cid)
        if not (ctry and ctry in allowed):
            continue

        if STRICT_LIVE:
            if st.get("status") == "online":
                live_ids.add(cid)
        else:
            # non-strict mode: any stream listing passes
            live_ids.add(cid)

    log.info(
        "Live channels in allowed countries: %d (STRICT=%s, has_status=%s)",
        len(live_ids), STRICT_LIVE, has_status
    )
    return live_ids

# =====================================================
# Core ingest
# =====================================================

def fetch_and_process_epg(sb: Client, urls: List[str]):
    now_utc     = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
    log.info("Window: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())

    live_channels = build_live_channel_set()

    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}

    dbg_any_title = 0
    dbg_any_desc  = 0
    dbg_good_title = 0
    dbg_good_desc  = 0

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                try:
                    resp.raise_for_status()
                except Exception as e:
                    log.warning("Skipping EPG URL due to HTTP error: %s (%s)", url, e)
                    continue

                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start", "end"))
                _, root = next(context)

                c_new = 0
                p_seen = 0
                p_kept = 0
                debug_left  = DEBUG_SAMPLE
                debug_filter: Set[str] = set(DEBUG_CHANNELS)

                for ev, el in context:
                    if ev != "end":
                        continue

                    tag = localname(el.tag)

                    # ---------- channels ----------
                    if tag == "channel":
                        ch_id = el.get("id")
                        if ch_id:
                            if live_channels and ch_id not in live_channels:
                                el.clear(); continue
                            # choose best display-name
                            names = all_text_nodes(el, "display-name")
                            name = pick_best_text(names) or ch_id
                            icon = icon_src(el)
                            if ch_id not in channels:
                                channels[ch_id] = {"id": ch_id, "display_name": name, "icon_url": icon}
                                c_new += 1
                        el.clear()
                        continue

                    # ---------- programmes ----------
                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel")
                        s = parse_xmltv_datetime(el.get("start"))
                        e = parse_xmltv_datetime(el.get("stop"))
                        if not (ch_id and s and e):
                            el.clear(); continue
                        # time window filter
                        if not (s <= horizon_utc and e >= now_utc):
                            el.clear(); continue
                        # live + country filter
                        if live_channels and ch_id not in live_channels:
                            el.clear(); continue

                        # collect texts
                        titles = all_text_nodes(el, "title")
                        subs   = all_text_nodes(el, "sub-title")
                        descs  = all_text_nodes(el, "desc")

                        if titles or subs: dbg_any_title += 1
                        if descs:          dbg_any_desc  += 1

                        # DEBUG sampler
                        if debug_left > 0 and (not debug_filter or (ch_id in debug_filter)):
                            log.info(
                                "DEBUG programme for channel=%s start=%s stop=%s\n"
                                "  titles: %s\n  sub-titles: %s\n  descs: %s\n  raw: %s",
                                ch_id, s.isoformat(), e.isoformat(),
                                titles, subs, descs, short_xml(el)
                            )
                            debug_left -= 1

                        # choose title/desc
                        title = pick_best_text(titles + subs) or "No Title"
                        desc  = pick_best_text(descs)

                        if title and title.strip().lower() not in JUNK_VALUES:
                            dbg_good_title += 1
                        if desc and desc.strip():
                            dbg_good_desc += 1

                        # Skip empties: no usable title AND no description
                        junk_title = (not title) or (title.strip().lower() in JUNK_VALUES or title == "No Title")
                        junk_desc  = (not desc) or (not str(desc).strip())
                        if junk_title and junk_desc:
                            el.clear(); continue

                        pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                        row = {
                            "id": pid,
                            "channel_id": ch_id,
                            "start_time": s.isoformat(),
                            "end_time": e.isoformat(),
                            "title": None if junk_title else title,
                            "description": None if junk_desc else desc
                        }

                        prev = programs.get(pid)
                        if prev is None:
                            programs[pid] = row
                            p_kept += 1
                        else:
                            prev_t = (prev.get("title") or "").strip().lower()
                            cand_t = (row.get("title") or "").strip().lower()
                            prev_d = (prev.get("description") or "") or ""
                            cand_d = (row.get("description") or "") or ""
                            replace = False
                            if prev_t in ("no title", "title", "") and cand_t not in ("no title", "title", ""):
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

                log.info("Parsed file done: channels(new)=%d, programs_found=%d, programs_kept_%dh=%d",
                         c_new, p_seen, WINDOW_HOURS, p_kept)

        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

    # ensure all program.channel_id exist in channels
    referenced = {p["channel_id"] for p in programs.values()}
    missing = referenced.difference(channels.keys())
    if missing:
        log.warning("Creating %d placeholder channels for missing IDs.", len(missing))
        for ch in missing:
            channels[ch] = {"id": ch, "display_name": ch, "icon_url": None}

    # Upserts
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

    # Cleanup: drop old rows outside window
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # Refresh MV
    refresh_mv(sb)

    # Debug stats
    log.info(
        "DEBUG summary: programmes with ANY title/sub-title=%d, ANY desc=%d; "
        "GOOD title(after junk-filter)=%d, GOOD desc=%d",
        dbg_any_title, dbg_any_desc, dbg_good_title, dbg_good_desc
    )

    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))

# =====================================================
# Entrypoint
# =====================================================

def main() -> int:
    log.info("EPG ingest starting. URLs (%d): %s", len(EPG_URLS), ", ".join(EPG_URLS) if EPG_URLS else "(none provided)")
    log.info(
        "FILTER_LIVE=%s, STRICT_LIVE=%s, ALLOWED_COUNTRIES=%s, WINDOW_HOURS=%d, DEBUG_SAMPLE=%d, DEBUG_CHANNELS=%s",
        FILTER_LIVE, STRICT_LIVE, ",".join(ALLOWED_COUNTRIES), WINDOW_HOURS, DEBUG_SAMPLE,
        (",".join(DEBUG_CHANNELS) if DEBUG_CHANNELS else "(any)")
    )
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_epg(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
