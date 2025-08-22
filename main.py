#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, gzip, time, logging, itertools, random, re, unicodedata
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Set, Tuple

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# =====================================================
# Config
# =====================================================

WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))

# Countries you support (used to filter iptv-org channels with streams)
ALLOWED_COUNTRIES = {
    c.strip().upper() for c in os.environ.get(
        "ALLOWED_COUNTRIES", "PR,US,MX,ES,DE,CA,IT,GB,IE,CO,AU"
    ).split(",") if c.strip()
}

# Live gating: require channel to exist in iptv-org streams.json (no status exists)
FILTER_LIVE = os.environ.get("FILTER_LIVE", "1") not in ("0","false","False","")

# Debug
DEBUG_SAMPLE = int(os.environ.get("DEBUG_SAMPLE", "8"))
DEBUG_CHANNELS = {s.strip() for s in os.environ.get("DEBUG_CHANNELS", "").split(",") if s.strip()}

# Defaults: only countries epg.pw clearly lists. You can add more sources if you have them.
DEFAULT_EPG_URLS = [
    "https://epg.pw/xmltv/epg_US.xml.gz",
    "https://epg.pw/xmltv/epg_CA.xml.gz",
    "https://epg.pw/xmltv/epg_DE.xml.gz",
    "https://epg.pw/xmltv/epg_GB.xml.gz",
    "https://epg.pw/xmltv/epg_AU.xml.gz",
    "https://epg.pw/xmltv/epg_FR.xml.gz",
]
_raw_urls = os.environ.get("EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] or list(DEFAULT_EPG_URLS)

# iptv-org API
IPTVORG_CHANNELS_URL = os.environ.get("IPTVORG_CHANNELS_URL", "https://iptv-org.github.io/api/channels.json")
IPTVORG_STREAMS_URL  = os.environ.get("IPTVORG_STREAMS_URL",  "https://iptv-org.github.io/api/streams.json")

REQUEST_TIMEOUT = (10, 180)  # (connect, read)
BATCH_CHANNELS = int(os.environ.get("BATCH_CHANNELS", "2000"))
BATCH_PROGRAMS = int(os.environ.get("BATCH_PROGRAMS", "1000"))
MAX_RETRIES = 4

# MV refresh
REFRESH_MV = os.environ.get("REFRESH_MV", "1") not in ("0","false","False","")
MV_FUNCS = [os.environ.get("REFRESH_MV_FUNC") or "refresh_programs_next_24h", "refresh_programs_next_12h"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# =====================================================
# Small utils
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

def open_xml_stream(resp: requests.Response, url: str):
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    gz = url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct
    return gzip.GzipFile(fileobj=resp.raw) if gz else resp.raw

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw: return None
    s = raw.strip()
    if " " in s:  # trim space before tz
        a, b = s.rsplit(" ", 1); s = a + b
    if len(s) >= 6 and (not s.endswith("Z")) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]
    if s.endswith("Z"): s = s[:-1] + "+0000"
    if len(s) == 14: s += "+0000"
    try:
        return datetime.strptime(s, "%Y%m%d%H%M%S%z").astimezone(timezone.utc)
    except Exception:
        return None

def localname(tag: str) -> str:
    if not tag: return tag
    return tag.split('}', 1)[1] if tag[0] == '{' else tag

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
            t = text_from(child)
            if t: return t
    return ''

def icon_src(elem: ET.Element) -> Optional[str]:
    for ic in iter_children(elem, "icon"):
        for k, v in ic.attrib.items():
            if localname(k).lower() == "src" and v:
                return v.strip()
    return None

def short_xml(elem: ET.Element, max_len: int = 2000) -> str:
    try: s = ET.tostring(elem, encoding="unicode")
    except Exception: s = "<unserializable>"
    return s if len(s) <= max_len else (s[:max_len] + "…")

# =====================================================
# Name normalization / matching
# =====================================================

STOP = {
    "tv","hd","uhd","sd","channel","ch","the","uk","us","usa","de","au","ca","es","mx","it","ie",
    "network","west","east","+1","+2","+3","fhd","hq","4k","1080p","720p","sd","hd+"
}
TOKEN_RE = re.compile(r"[^\w]+")

def norm(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"\(.*?\)", " ", s)            # drop parentheses content
    s = re.sub(r"\bhd\b|\bsd\b|\bfhd\b", " ", s)
    s = ' '.join(s.split())
    return s

def tokens(s: str) -> Set[str]:
    tks = [t for t in TOKEN_RE.split(norm(s)) if t]
    return {t for t in tks if len(t) > 1 and t not in STOP}

def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b: return 0.0
    inter = len(a & b); union = len(a | b)
    return inter / union if union else 0.0

# =====================================================
# iptv-org live (no status; presence in streams.json = has streams)
# =====================================================

def fetch_json(url: str) -> list:
    log.info("Fetching JSON: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

class LiveIndex:
    def __init__(self, ids: Set[str], names: Set[str], name_tokens: Dict[str, Set[str]]):
        self.ids = ids                  # lowercase iptv-org channel ids with streams in allowed countries
        self.names = names              # normalized names/alt_names
        self.name_tokens = name_tokens  # name -> tokens

def build_live_index() -> LiveIndex:
    if not FILTER_LIVE:
        return LiveIndex(set(), set(), {})

    chans = fetch_json(IPTVORG_CHANNELS_URL)   # has 'id','name','alt_names','country',...
    streams = fetch_json(IPTVORG_STREAMS_URL)  # has 'channel','url',... (NO 'status')  <-- API spec

    have_streams: Set[str] = {str(s.get("channel","")).lower() for s in streams if s.get("channel")}
    ids: Set[str] = set()
    names: Set[str] = set()
    name_tokens: Dict[str, Set[str]] = {}

    kept = 0
    for ch in chans:
        cid = (ch.get("id") or "").lower()
        if not cid: continue
        ctry = (ch.get("country") or "").upper()
        if ctry not in ALLOWED_COUNTRIES: continue
        if cid not in have_streams: continue

        ids.add(cid); kept += 1

        pool = [ch.get("name") or ""]
        alts = ch.get("alt_names") or []
        pool.extend([a for a in alts if a])

        for nm in pool:
            nn = norm(nm)
            if nn:
                names.add(nn)
                name_tokens[nn] = tokens(nn)

    log.info("iptv-org: live channels kept by country=%d; unique names=%d", kept, len(names))
    return LiveIndex(ids, names, name_tokens)

def is_live_epg_channel(epg_id: str, epg_display_names: List[str], live: LiveIndex) -> bool:
    # 1) match by id (many XMLTVs use iptv-org ids like 'ABC.us')
    if epg_id and epg_id.lower() in live.ids:
        return True

    # 2) direct name match
    for n in epg_display_names:
        nn = norm(n)
        if not nn: continue
        if nn in live.names:
            return True

    # 3) token/Jaccard match
    for n in epg_display_names:
        nn = norm(n)
        if not nn: continue
        t = tokens(nn)
        if not t: continue
        # quick prefilter: try a few likely candidates by shared tokens
        # (small N: brute-force is fine)
        for ln, ltok in live.name_tokens.items():
            if jaccard(t, ltok) >= 0.6:
                return True

    return False

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
                if not k: continue
                keep = dedup.get(k)
                if keep is None:
                    dedup[k] = r
                else:
                    kd, kd0 = (r.get("description") or ""), (keep.get("description") or "")
                    kt, kt0 = (r.get("title") or ""), (keep.get("title") or "")
                    replace = False
                    if norm(kt0) in {"", "title", "no title"} and norm(kt) not in {"", "title", "no title"}:
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
                    queue.insert(0, batch[mid:]); queue.insert(0, batch[:mid])
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
        log.info("Skipping MV refresh (REFRESH_MV disabled).")
        return
    for fn in MV_FUNCS:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                log.info("Refreshing materialized view via RPC: %s …", fn)
                sb.rpc(fn).execute()
                log.info("✅ Materialized view refreshed.")
                return
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.warning("RPC %s failed after %d attempts: %s", fn, attempt, e)
                    break
                sleep_s = attempt * rand_jitter()
                log.warning("Retry %d/%d for %s in %.2fs: %s", attempt, MAX_RETRIES, fn, sleep_s, e)
                time.sleep(sleep_s)
    log.error("❌ MV refresh failed via all candidates: %s", MV_FUNCS)

# =====================================================
# Core ingest
# =====================================================

JUNK_TITLES = {"", "title", "no title"}

def fetch_and_process_epg(sb: Client, urls: List[str]):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
    log.info("Window: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())

    live = build_live_index() if FILTER_LIVE else LiveIndex(set(), set(), {})

    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}
    allowed_epg_channels: Set[str] = set()

    # Debug collections
    dbg_kept, dbg_notlive, dbg_empty = [], [], []

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status_code == 404:
                    log.warning("EPG URL 404: %s (skipping)", url)
                    continue
                resp.raise_for_status()

                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start","end"))
                _, root = next(context)

                c_seen = c_kept = p_seen = p_kept = 0
                debug_left = DEBUG_SAMPLE

                for ev, el in context:
                    if ev != "end": continue
                    tag = localname(el.tag)

                    if tag == "channel":
                        c_seen += 1
                        ch_id = el.get("id") or ""
                        names = [text_from(dn) for dn in iter_children(el, "display-name") if text_from(dn)]
                        icon = icon_src(el)

                        keep = True
                        if FILTER_LIVE:
                            keep = is_live_epg_channel(ch_id, names, live)

                        if keep:
                            display = names[0] if names else ch_id
                            channels[ch_id] = {"id": ch_id, "display_name": display, "icon_url": icon}
                            allowed_epg_channels.add(ch_id)
                            c_kept += 1
                        else:
                            if len(dbg_notlive) < DEBUG_SAMPLE:
                                dbg_notlive.append(f"EPG channel SKIPPED (not live): id={ch_id} names={names[:2]}")
                        el.clear(); continue

                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel") or ""
                        if FILTER_LIVE and ch_id not in allowed_epg_channels:
                            if len(dbg_notlive) < DEBUG_SAMPLE and (not DEBUG_CHANNELS or ch_id in DEBUG_CHANNELS):
                                dbg_notlive.append(f"programme SKIPPED (channel not live): ch={ch_id} raw={short_xml(el)[:300]}")
                            el.clear(); continue

                        s = parse_xmltv_datetime(el.get("start"))
                        e = parse_xmltv_datetime(el.get("stop"))
                        if not (ch_id and s and e):
                            el.clear(); continue
                        if not (s <= horizon_utc and e >= now_utc):
                            el.clear(); continue

                        title = (first_text_by_names(el, "title", "sub-title") or "").strip()
                        desc  = (first_text_by_names(el, "desc") or "").strip()

                        if norm(title) in JUNK_TITLES:
                            if len(dbg_empty) < DEBUG_SAMPLE and (not DEBUG_CHANNELS or ch_id in DEBUG_CHANNELS):
                                dbg_empty.append(f"programme SKIPPED (empty title): ch={ch_id} start={s} raw={short_xml(el)[:300]}")
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
                                dbg_kept.append(f"programme KEPT: ch={ch_id} start={s} title={title!r} desc_len={len(desc)}")
                        else:
                            # prefer better title/longer desc
                            prev_t, cand_t = prev.get("title") or "", row.get("title") or ""
                            prev_d, cand_d = prev.get("description") or "", row.get("description") or ""
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

    # Ensure referential integrity
    referenced = {p["channel_id"] for p in programs.values()}
    missing = referenced.difference(channels.keys())
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
        log.warning("No programmes kept (check feeds or live filter).")

    # Verify, cleanup, MV refresh
    cnt = count_programs_in_window(sb, now_utc, horizon_utc)
    if cnt >= 0:
        log.info("✅ Supabase now has %d programs in the %dh window.", cnt, WINDOW_HOURS)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    refresh_mv(sb)

    # Debug summaries
    for s in dbg_kept[:DEBUG_SAMPLE]: log.info(s)
    for s in dbg_empty[:DEBUG_SAMPLE]: log.info(s)
    for s in dbg_notlive[:DEBUG_SAMPLE]: log.info(s)

    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))

# =====================================================
# Entrypoint
# =====================================================

def main() -> int:
    log.info("EPG ingest starting. URLs (%d): %s", len(EPG_URLS), ", ".join(EPG_URLS) if EPG_URLS else "(none)")
    log.info("FILTER_LIVE=%s, ALLOWED_COUNTRIES=%s, WINDOW_HOURS=%d, DEBUG_SAMPLE=%d, DEBUG_CHANNELS=%s",
             FILTER_LIVE, ",".join(sorted(ALLOWED_COUNTRIES)), WINDOW_HOURS, DEBUG_SAMPLE,
             "(any)" if not DEBUG_CHANNELS else ",".join(sorted(DEBUG_CHANNELS)))
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_epg(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
