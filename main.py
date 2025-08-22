#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, gzip, time, logging, itertools, random, re, unicodedata
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Tuple, Set

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

# Countries you support in your app
ALLOWED_COUNTRIES = tuple(os.environ.get("ALLOWED_COUNTRIES", "PR,US,MX,ES,DE,CA,IT,GB,IE,CO,AU").split(","))

# Default to the epgshare mega dump (has names; we will filter hard afterward).
DEFAULT_EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
]

REQUEST_TIMEOUT = (10, 180)
BATCH_CHANNELS   = 2000
BATCH_PROGRAMS   = 1000
MAX_RETRIES      = 4

WINDOW_HOURS     = int(os.environ.get("WINDOW_HOURS", "12"))

# Toggle live/country filtering (should remain True for your use-case)
FILTER_LIVE      = os.environ.get("FILTER_LIVE", "1") not in ("0","false","False","")
# If True and we can't map a channel to iptv-org, we skip it (prevents DB bloat on nameless feeds)
STRICT_LIVE      = os.environ.get("STRICT_LIVE", "1") not in ("0","false","False","")

# Debug sampler
DEBUG_SAMPLE     = int(os.environ.get("DEBUG_SAMPLE", "8"))
DEBUG_CHANNELS   = set([s.strip().lower() for s in os.environ.get("DEBUG_CHANNELS","").split(",") if s.strip()])

# Materialized view RPC names (we'll try 12h then 24h)
MV_RPC_12H       = os.environ.get("MV_RPC_12H", "refresh_programs_next_12h")
MV_RPC_24H       = os.environ.get("MV_RPC_24H", "refresh_programs_next_24h")
REFRESH_MV       = os.environ.get("REFRESH_MV", "1") not in ("0","false","False","")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg")

SUPABASE_URL        = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY= os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_EPG_URLS)

# ----------------------- Small helpers -----------------------

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
    if not raw: return None
    s = raw.strip()

    # remove accidental space before tz (e.g. '...  +0200')
    if " " in s:
        a, b = s.rsplit(" ", 1)
        s = a + b

    # normalize +HH:MM -> +HHMM
    if len(s) >= 6 and (not s.endswith("Z")) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]

    # trailing 'Z' -> '+0000'
    if s.endswith("Z"):
        s = s[:-1] + "+0000"

    # add UTC if no tz
    if len(s) == 14:  # YYYYMMDDHHMMSS
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
    if not tag: return tag
    if tag[0] == '{':
        return tag.split('}', 1)[1]
    return tag

def text_from(elem: Optional[ET.Element]) -> str:
    return ''.join(elem.itertext()).strip() if elem is not None else ''

def find_child(elem: ET.Element, name: str) -> Optional[ET.Element]:
    lname = name.lower()
    for child in list(elem):
        if localname(child.tag).lower() == lname:
            return child
    return None

def icon_src(elem: ET.Element) -> Optional[str]:
    ic = find_child(elem, 'icon')
    if ic is None: return None
    for k, v in ic.attrib.items():
        if localname(k).lower() == 'src' and v:
            return v.strip()
    return None

def norm(s: str) -> str:
    """Normalize a name for fuzzy matching."""
    s = s.strip().lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'[\s\.\-_/]+', '', s)
    s = re.sub(r'(?i)\b(hd|sd|uhd|4k)\b', '', s)
    s = s.replace('&','and')
    return s

def strip_group_prefix(ch_id: str) -> str:
    """Remove common 'NN.' or provider prefixes in channel @id (e.g., '21.Junior.al' -> 'Junior.al')."""
    return re.sub(r'^\d+\.', '', ch_id or '')

# ----------------------- Supabase -----------------------

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
                if not k: continue
                keep = dedup.get(k)
                if keep is None:
                    dedup[k] = r
                else:
                    kd  = (r.get("description") or "")
                    kd0 = (keep.get("description") or "")
                    kt  = (r.get("title") or "")
                    kt0 = (keep.get("title") or "")
                    replace = False
                    if kt0.strip() in ("", "No Title", "Title") and kt.strip() not in ("", "No Title", "Title"):
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
                need_split = any(k in msg for k in ("21000","duplicate key value violates","500","413","Payload"))
                if need_split and len(batch) > 1:
                    mid = len(batch)//2
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

def refresh_mv(sb: Client) -> None:
    if not REFRESH_MV: 
        log.info("Skipping materialized view refresh (REFRESH_MV disabled).")
        return
    # Prefer the 12h RPC first (that’s what you created), then try 24h fallback.
    for fn in (MV_RPC_12H, MV_RPC_24H):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                log.info("Refreshing materialized view via RPC: %s …", fn)
                sb.rpc(fn).execute()
                log.info("✅ Materialized view refreshed.")
                return
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.warning("RPC %s failed after %d attempts: %s", fn, attempt, e)
                else:
                    sleep_s = attempt * rand_jitter()
                    log.warning("Retry %d/%d for %s in %.2fs: %s", attempt, MAX_RETRIES, fn, sleep_s, e)
                    time.sleep(sleep_s)

# ----------------------- iptv-org live/country filters -----------------------

def fetch_json(url: str):
    log.info("Fetching JSON: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def load_live_sets(allowed_countries: Iterable[str]) -> Tuple[Dict[str, str], Dict[str, Set[str]], Set[str]]:
    """
    Returns:
      channel_country: iptv-org channel_id -> country (filtered)
      name_to_channel: normalized name -> set(channel_id)
      live_channels: set of channel_ids that have at least one stream
    """
    countries = {c.strip().upper() for c in allowed_countries}
    # iptv-org APIs
    channels = fetch_json("https://iptv-org.github.io/api/channels.json")     # fields include id, name, alt_names, country …
    streams  = fetch_json("https://iptv-org.github.io/api/streams.json")      # fields include channel, title, url … (no status)
    # Build channel country map
    channel_country: Dict[str, str] = {}
    for ch in channels:
        cid = ch.get("id"); ctry = (ch.get("country") or "").upper()
        if not cid or ctry not in countries: continue
        channel_country[cid] = ctry
    # Build normalized names map (names + alt_names + stream titles)
    name_to_channel: Dict[str, Set[str]] = {}
    def add_name(n: str, cid: str):
        if not n: return
        k = norm(n)
        if not k: return
        name_to_channel.setdefault(k, set()).add(cid)
    for ch in channels:
        cid = ch.get("id"); ctry = (ch.get("country") or "").upper()
        if cid in channel_country and ctry in countries:
            add_name(ch.get("name") or "", cid)
            for alt in ch.get("alt_names") or []:
                add_name(alt, cid)
    for st in streams:
        cid = st.get("channel")
        if cid in channel_country:
            add_name(st.get("title") or "", cid)

    # A channel is "live" if there exists any stream pointing to it
    live_channels: Set[str] = set()
    for st in streams:
        cid = st.get("channel")
        if cid in channel_country:
            live_channels.add(cid)

    log.info("iptv-org: live channels kept by country=%d; unique names=%d", len(live_channels), len(name_to_channel))
    return channel_country, name_to_channel, live_channels

def epg_channel_is_live(epg_id: str, epg_names: List[str],
                        name_to_channel: Dict[str, Set[str]],
                        live_channels: Set[str],
                        channel_country: Dict[str, str]) -> bool:
    """
    Decide if this EPG channel should be kept.
    1) try by normalized display-name/title mapping to iptv-org channel id(s)
    2) try by stripping common prefixes from the EPG @id and matching that as a "name"
    Pass only if ANY mapped iptv-org channel is in live_channels.
    """
    # Check normalized names
    for nm in epg_names:
        key = norm(nm)
        if not key: continue
        cids = name_to_channel.get(key)
        if not cids: continue
        if any(cid in live_channels for cid in cids):
            return True

    # Fallback: sometimes @id contains something close to the database id or a usable label
    fallback = strip_group_prefix(epg_id or "")
    if fallback:
        key = norm(fallback)
        cids = name_to_channel.get(key)
        if cids and any(cid in live_channels for cid in cids):
            return True

    return False

# ----------------------- Core ingest -----------------------

JUNK_TITLES = {"", "title", "no title", "untitled", "program", "programme", "n/a", "-"}

def fetch_and_process_epg(sb: Client, urls: List[str]):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
    log.info("Window: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())

    # Build iptv-org derived filters
    channel_country, name_to_channel, live_channels = load_live_sets(ALLOWED_COUNTRIES)

    channels: Dict[str, dict] = {}   # id -> row
    programs: Dict[str, dict] = {}   # id -> row
    debug_count = 0
    dbg_any_title = dbg_any_desc = 0
    dbg_good_title = dbg_good_desc = 0

    session = requests.Session()

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status_code == 404:
                    log.warning("EPG URL 404: %s (skipping)", url)
                    continue
                resp.raise_for_status()
                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start","end"))
                _, root = next(context)

                # temp store of EPG channel id -> names/icon
                epg_ch_names: Dict[str, List[str]] = {}
                epg_ch_icon: Dict[str, Optional[str]] = {}
                epg_ch_keep: Dict[str, bool] = {}

                p_seen = p_kept = 0
                c_seen = c_kept = 0

                for ev, el in context:
                    if ev != "end":
                        continue

                    tag = localname(el.tag)

                    if tag == "channel":
                        c_seen += 1
                        ch_id = el.get("id") or ""
                        if not ch_id:
                            el.clear(); continue
                        # collect all display-names (may be multiple/lang)
                        names = []
                        for child in list(el):
                            if localname(child.tag).lower() == "display-name":
                                t = text_from(child)
                                if t: names.append(t)
                        if not names:
                            # sometimes id is the only usable "name" (we'll try)
                            pass
                        epg_ch_names[ch_id] = names
                        epg_ch_icon[ch_id]  = icon_src(el)

                        # live/country filter
                        keep = True
                        if FILTER_LIVE:
                            keep = epg_channel_is_live(ch_id, names, name_to_channel, live_channels, channel_country)
                        epg_ch_keep[ch_id] = keep
                        if keep:
                            # pick a display name for channels table
                            disp = names[0] if names else strip_group_prefix(ch_id) or ch_id
                            channels.setdefault(ch_id, {"id": ch_id, "display_name": disp, "icon_url": epg_ch_icon[ch_id]})
                            c_kept += 1
                        else:
                            log.info("EPG channel SKIPPED (not live): id=%s names=%s", ch_id, names)
                        el.clear()
                        continue

                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel") or ""
                        if not ch_id:
                            el.clear(); continue
                        if FILTER_LIVE and not epg_ch_keep.get(ch_id, False):
                            el.clear(); continue

                        s = parse_xmltv_datetime(el.get("start"))
                        e = parse_xmltv_datetime(el.get("stop"))
                        if not (s and e):
                            el.clear(); continue
                        if not (s <= horizon_utc and e >= now_utc):
                            el.clear(); continue

                        titles, subtitles, descs = [], [], []
                        for child in list(el):
                            lname = localname(child.tag).lower()
                            if lname == "title":
                                t = text_from(child); titles.append(t)
                            elif lname == "sub-title":
                                t = text_from(child); subtitles.append(t)
                            elif lname == "desc":
                                t = text_from(child); descs.append(t)

                        # Debug sampler: print a few raw samples to see what the feed contains
                        if debug_count < DEBUG_SAMPLE and (not DEBUG_CHANNELS or ch_id.lower() in DEBUG_CHANNELS):
                            debug_count += 1
                            log.info(
                                "DEBUG programme for channel=%s start=%s stop=%s\n  titles=%s\n  sub-titles=%s\n  descs=%s\n  raw=%s\n",
                                ch_id, s.isoformat(), e.isoformat(),
                                [(t, norm(t)) for t in titles],
                                [(t, norm(t)) for t in subtitles],
                                [(d, norm(d)) for d in descs],
                                ET.tostring(el, encoding="unicode")
                            )

                        any_title = any((t or "").strip() for t in titles+subtitles)
                        any_desc  = any((d or "").strip() for d in descs)
                        if any_title: dbg_any_title += 1
                        if any_desc:  dbg_any_desc  += 1

                        # Choose best title / desc
                        title = ""
                        for src in (titles + subtitles):
                            t = (src or "").strip()
                            if t and norm(t) not in JUNK_TITLES:
                                title = t; break
                        if title: dbg_good_title += 1
                        desc = ""
                        for d in descs:
                            d = (d or "").strip()
                            if d:
                                desc = d; break
                        if desc: dbg_good_desc += 1

                        # Skip if title still junk/empty
                        if not title or norm(title) in JUNK_TITLES:
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

                        # Dedup policy: prefer longer description / non-junk title
                        prev = programs.get(pid)
                        if prev is None:
                            programs[pid] = row
                            p_kept += 1
                        else:
                            prev_t = (prev.get("title") or "")
                            cand_t = title
                            prev_d = (prev.get("description") or "") or ""
                            cand_d = (row.get("description") or "") or ""
                            replace = False
                            if norm(prev_t) in JUNK_TITLES and norm(cand_t) not in JUNK_TITLES:
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
    for sample in prog_rows[:3]:
        log.info("Sample program row: %s", {k: sample[k] for k in ("id","channel_id","start_time","end_time","title")})
    if prog_rows:
        prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programmes kept (check feeds or live filter).")

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

    # refresh materialized view
    refresh_mv(sb)

    log.info("DEBUG summary: programmes with ANY title/sub-title=%d, ANY desc=%d; GOOD title(after junk-filter)=%d, GOOD desc=%d",
             dbg_any_title, dbg_any_desc, dbg_good_title, dbg_good_desc)
    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))

# ----------------------- Entrypoint -----------------------

def main() -> int:
    log.info("EPG ingest starting. URLs (%d): %s", len(EPG_URLS), ", ".join(EPG_URLS) if EPG_URLS else "(none provided)")
    log.info("FILTER_LIVE=%s, ALLOWED_COUNTRIES=%s, WINDOW_HOURS=%d, DEBUG_SAMPLE=%d, DEBUG_CHANNELS=%s",
             FILTER_LIVE, ",".join(ALLOWED_COUNTRIES), WINDOW_HOURS, DEBUG_SAMPLE,
             "(any)" if not DEBUG_CHANNELS else ",".join(sorted(DEBUG_CHANNELS)))
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_epg(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
