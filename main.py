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

# ==================== Config ====================

WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))

# Puerto Rico pilot
COUNTRY = os.environ.get("COUNTRY", "PR").upper()

# If a programme time has **no timezone**, assume this offset.
# For Puerto Rico the local offset is UTC-4 (no DST).
DEFAULT_NAIVE_TZ = os.environ.get("DEFAULT_NAIVE_TZ", "-0400")

FILTER_LIVE = True  # require at least one iptv-org stream for COUNTRY
REFRESH_MV_FUNC = os.environ.get("REFRESH_MV_FUNC", "refresh_programs_next_12h")

REQUEST_TIMEOUT = (10, 180)
BATCH_CHANNELS = 2000
BATCH_PROGRAMS = 1000
MAX_RETRIES = 4

# Open-EPG discovery for PR (we’ll HEAD these and also scrape the guide page)
OPEN_EPG_INDEX = "https://www.open-epg.com/app/epgguide.php"
OPEN_EPG_CANDIDATES = [
    "https://www.open-epg.com/files/puertorico1.xml.gz",
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml.gz",
    "https://www.open-epg.com/files/puertorico2.xml",
]

IPTV_CHANNELS_URL = "https://iptv-org.github.io/api/channels.json"
IPTV_STREAMS_URL  = "https://iptv-org.github.io/api/streams.json"

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# Debug knobs
DEBUG_SAMPLE           = int(os.environ.get("DEBUG_SAMPLE", "12"))
DEBUG_LOG_UNPARSEABLE  = int(os.environ.get("DEBUG_LOG_UNPARSEABLE", "6"))
DEBUG_LOG_OUTOFWINDOW  = int(os.environ.get("DEBUG_LOG_OUTOFWINDOW", "6"))
DEBUG_LOG_JUNKTITLE    = int(os.environ.get("DEBUG_LOG_JUNKTITLE", "6"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg-pr")

# ==================== Helpers ====================

def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, size))
        if not block: return
        yield block

def rand_jitter() -> float:
    return 0.25 + random.random() * 0.75

def open_xml_stream(resp: requests.Response):
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    gz = ("gzip" in ct) or resp.request.url.lower().endswith(".gz")
    return gzip.GzipFile(fileobj=resp.raw) if gz else resp.raw

# ---------- Flexible XMLTV datetime parsing ----------

_DT_PATTERNS = (
    # compact with tz or Z
    ("%Y%m%d%H%M%S%z", None),
    ("%Y%m%d%H%M%S", "compact"),
    # dashed with tz or Z
    ("%Y-%m-%d %H:%M:%S%z", None),
    ("%Y-%m-%d %H:%M:%S", "dashed"),
)

def _normalize_tz_tail(s: str) -> str:
    """
    Convert '+HH:MM' -> '+HHMM', 'Z' -> '+0000', strip stray space before tz.
    """
    s = s.strip()
    if " " in s:
        a, b = s.rsplit(" ", 1)
        # handle Z / +HH:MM / +HHMM
        if b.upper() == "Z":
            return a + " +0000"
        if re.fullmatch(r"[+-]\d{2}:\d{2}", b):
            return a + " " + b.replace(":", "")
    elif s.endswith("Z"):
        return s[:-1] + "+0000"
    return s

def parse_xmltv_datetime(raw: Optional[str], naive_tz: str = DEFAULT_NAIVE_TZ) -> Optional[datetime]:
    """Parse many common XMLTV datetime flavors; attach `naive_tz` if no tz present."""
    if not raw: return None
    s = _normalize_tz_tail(raw)

    # First, try all patterns as-given
    for fmt, kind in _DT_PATTERNS:
        try:
            if kind is None:
                dt = datetime.strptime(s, fmt)
                return dt.astimezone(timezone.utc)
            else:
                # naive form; attach provided offset
                dt = datetime.strptime(s, fmt)
                # naive -> pretend it was in local offset, convert to UTC
                # naive_tz like "-0400" or "+0530"
                m = re.fullmatch(r"([+-])(\d{2})(\d{2})", naive_tz.strip())
                if not m:
                    # fallback to UTC if misconfigured
                    return dt.replace(tzinfo=timezone.utc)
                sign, hh, mm = m.groups()
                offset_minutes = int(hh) * 60 + int(mm)
                if sign == "-": offset_minutes = -offset_minutes
                tz = timezone(timedelta(minutes=offset_minutes))
                return dt.replace(tzinfo=tz).astimezone(timezone.utc)
        except Exception:
            continue

    # Try compactifying if there is a trailing tz with colon but no space (rare)
    try:
        m = re.match(r"^(\d{8}\d{6})[ ]?([+-]\d{2}):?(\d{2})$", s)
        if m:
            core, hh, mm = m.groups()
            dt = datetime.strptime(core + hh + mm, "%Y%m%d%H%M%S%z")
            return dt.astimezone(timezone.utc)
    except Exception:
        pass

    return None

# ---------- XML helpers ----------

def localname(tag: str) -> str:
    if not tag: return tag
    return tag.split("}", 1)[1] if tag.startswith("{") else tag

def text_from(el: Optional[ET.Element]) -> str:
    return "".join(el.itertext()).strip() if el is not None else ""

def icon_src(channel_el: ET.Element) -> Optional[str]:
    for child in list(channel_el):
        if localname(child.tag).lower() == "icon":
            for k, v in child.attrib.items():
                if localname(k).lower() == "src" and v:
                    return v.strip()
    return None

# ---------- Name normalization ----------

STOP = {"tv","hd","uhd","sd","channel","ch","the","pr","puerto","rico","+1","+2","fhd","4k"}
TOKEN_RE = re.compile(r"[^\w]+")

def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = s.replace("&", " and ")
    s = " ".join(s.split())
    return s

def tokens(s: str) -> Set[str]:
    return {t for t in TOKEN_RE.split(norm_name(s)) if t and t not in STOP and len(t) > 1}

JUNK_TITLES = {"", "title", "no title", "untitled", "-", "programme", "program"}

# ==================== iptv-org live set (COUNTRY) ====================

def fetch_json(url: str):
    log.info("Fetching JSON: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def build_live_country(country_code: str) -> Tuple[Dict[str, Set[str]], Set[str]]:
    """
    Returns:
      name_index: normalized name -> set(channel_ids)
      live_ids: set of iptv-org channel ids that have at least one stream and match country
    """
    country_code = country_code.upper()
    channels = fetch_json(IPTV_CHANNELS_URL)
    streams  = fetch_json(IPTV_STREAMS_URL)

    country_ids = {ch["id"] for ch in channels if (ch.get("country") or "").upper() == country_code}
    live_ids = {s.get("channel") for s in streams if s.get("channel") in country_ids}

    name_index: Dict[str, Set[str]] = {}
    def add_name(nm: str, cid: str):
        key = norm_name(nm)
        if not key: return
        name_index.setdefault(key, set()).add(cid)

    for ch in channels:
        cid = ch.get("id")
        if cid not in live_ids: continue
        add_name(ch.get("name") or "", cid)
        for alt in ch.get("alt_names") or []:
            add_name(alt, cid)

    for st in streams:
        cid = st.get("channel")
        if cid in live_ids:
            add_name(st.get("title") or "", cid)

    log.info("iptv-org %s: live_ids=%d, name_keys=%d", country_code, len(live_ids), len(name_index))
    return name_index, live_ids

# ==================== Discover Open-EPG PR URLs ====================

def discover_open_epg_pr() -> List[str]:
    urls: List[str] = []
    session = requests.Session()
    for u in OPEN_EPG_CANDIDATES:
        try:
            r = session.head(u, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                urls.append(u)
        except Exception:
            pass
    try:
        r = session.get(OPEN_EPG_INDEX, timeout=REQUEST_TIMEOUT)
        if r.ok:
            m = re.findall(r"https://www\.open-epg\.com/files/puertorico\d+\.xml(?:\.gz)?", r.text, flags=re.I)
            for u in m:
                if u not in urls:
                    urls.append(u)
    except Exception:
        pass
    urls = list(dict.fromkeys(urls))
    log.info("Open-EPG PR URLs discovered: %s", ", ".join(urls) if urls else "(none)")
    return urls

# ==================== Supabase ====================

def init_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("✅ Connected to Supabase.")
        return sb
    except Exception as e:
        log.exception("Failed to create Supabase client: %s", e); sys.exit(1)

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
                prev = dedup.get(k)
                if prev is None:
                    dedup[k] = r
                else:
                    kt0, kt = prev.get("title") or "", r.get("title") or ""
                    kd0, kd = prev.get("description") or "", r.get("description") or ""
                    replace = False
                    if norm_name(kt0) in JUNK_TITLES and norm_name(kt) not in JUNK_TITLES:
                        replace = True
                    elif len(kd) > len(kd0):
                        replace = True
                    if replace: dedup[k] = r
            batch = list(dedup.values())

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch); break
            except APIError as e:
                msg = str(e)
                need_split = any(s in msg for s in ("21000","duplicate key value violates","500","413","Payload"))
                if need_split and len(batch) > 1:
                    mid = len(batch)//2
                    queue.insert(0, batch[mid:]); queue.insert(0, batch[:mid])
                    log.warning("Splitting %s batch (%d) due to error: %s", table, len(batch), msg); break
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

def refresh_mv(sb: Client):
    try:
        log.info("Refreshing MV via RPC: %s …", REFRESH_MV_FUNC)
        sb.rpc(REFRESH_MV_FUNC).execute()
        log.info("✅ MV refreshed.")
    except Exception as e:
        log.warning("MV refresh failed: %s", e)

# ==================== Core ingest (PR) ====================

def fetch_and_process_pr(sb: Client):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
    log.info("Window: %s -> %s (UTC)  (naive times => %s)",
             now_utc.isoformat(), horizon_utc.isoformat(), DEFAULT_NAIVE_TZ)

    name_index, live_ids = build_live_country(COUNTRY)
    if FILTER_LIVE and not live_ids:
        log.warning("No live ids found for %s in iptv-org; aborting.", COUNTRY); return

    epg_urls = discover_open_epg_pr()
    if not epg_urls:
        log.warning("No Open-EPG files found for PR; aborting."); return

    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}

    # Debug counters
    dbg_unparseable = 0
    dbg_out_of_window = 0
    dbg_junk_title = 0

    def is_live_match(names: List[str], ch_id_text: str) -> bool:
        for nm in names + [ch_id_text]:
            key = norm_name(nm)
            if key in name_index: return True
        return False

    for url in epg_urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status_code == 404:
                    log.warning("404 for %s, skipping", url); continue
                resp.raise_for_status()
                stream = open_xml_stream(resp)
                context = ET.iterparse(stream, events=("start","end"))
                _, root = next(context)

                keep_channel: Dict[str, bool] = {}
                ch_names: Dict[str, List[str]] = {}
                ch_icon: Dict[str, Optional[str]] = {}

                c_seen = c_kept = p_seen = p_kept = 0

                for ev, el in context:
                    if ev != "end": continue
                    tag = localname(el.tag)

                    if tag == "channel":
                        c_seen += 1
                        ch_id = el.get("id") or ""
                        names = []
                        for child in list(el):
                            if localname(child.tag).lower() == "display-name":
                                t = text_from(child)
                                if t: names.append(t)
                        ch_names[ch_id] = names
                        ch_icon[ch_id] = icon_src(el)

                        keep = True
                        if FILTER_LIVE:
                            keep = is_live_match(names, ch_id)
                        keep_channel[ch_id] = keep
                        if keep:
                            disp = names[0] if names else ch_id
                            channels[ch_id] = {"id": ch_id, "display_name": disp, "icon_url": ch_icon[ch_id]}
                            c_kept += 1
                        el.clear(); continue

                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel") or ""
                        if FILTER_LIVE and not keep_channel.get(ch_id, False):
                            el.clear(); continue

                        raw_start = el.get("start")
                        raw_stop  = el.get("stop")
                        s = parse_xmltv_datetime(raw_start)
                        e = parse_xmltv_datetime(raw_stop)

                        if not (s and e):
                            if dbg_unparseable < DEBUG_LOG_UNPARSEABLE:
                                log.info("DEBUG skip (unparseable time): ch=%s start=%s stop=%s raw=%s",
                                         ch_id, raw_start, raw_stop, ET.tostring(el, encoding="unicode")[:300])
                            dbg_unparseable += 1
                            el.clear(); continue

                        if not (s <= horizon_utc and e >= now_utc):
                            if dbg_out_of_window < DEBUG_LOG_OUTOFWINDOW:
                                log.info("DEBUG skip (outside window): ch=%s start=%s stop=%s now=%s horizon=%s",
                                         ch_id, s.isoformat(), e.isoformat(),
                                         now_utc.isoformat(), horizon_utc.isoformat())
                            dbg_out_of_window += 1
                            el.clear(); continue

                        title = ""
                        desc = ""
                        for child in list(el):
                            nm = localname(child.tag).lower()
                            if nm == "title" and not title:
                                t = text_from(child).strip()
                                if t and norm_name(t) not in JUNK_TITLES: title = t
                            elif nm == "sub-title" and not title:
                                t = text_from(child).strip()
                                if t and norm_name(t) not in JUNK_TITLES: title = t
                            elif nm == "desc" and not desc:
                                d = text_from(child).strip()
                                if d: desc = d

                        if not title or norm_name(title) in JUNK_TITLES:
                            if dbg_junk_title < DEBUG_LOG_JUNKTITLE:
                                log.info("DEBUG skip (junk/empty title): ch=%s start=%s title=%r", ch_id, s.isoformat(), title)
                            dbg_junk_title += 1
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
                        else:
                            prev_t, prev_d = prev.get("title",""), prev.get("description") or ""
                            replace = False
                            if norm_name(prev_t) in JUNK_TITLES and norm_name(title) not in JUNK_TITLES:
                                replace = True
                            elif len(desc) > len(prev_d):
                                replace = True
                            if replace: programs[pid] = row

                        el.clear()
                        if (p_kept % 8000) == 0:
                            root.clear()
                        continue

                    el.clear()

                log.info("Parsed %s: channels(seen)=%d kept=%d, programs_found=%d kept_%dh=%d",
                         url, c_seen, c_kept, p_seen, WINDOW_HOURS, p_kept)

        except Exception as e:
            log.warning("Failed %s: %s", url, e)

    # Ensure referential integrity
    referenced = {p["channel_id"] for p in programs.values()}
    for ch_id in referenced:
        if ch_id not in channels:
            channels[ch_id] = {"id": ch_id, "display_name": ch_id, "icon_url": None}

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
        log.warning("No programmes kept (check matching/window).")

    # Verify, cleanup, refresh
    try:
        res = sb.table("programs").select("id", count="exact")\
            .gte("end_time", now_utc.isoformat())\
            .lte("start_time", horizon_utc.isoformat())\
            .execute()
        cnt = getattr(res, "count", None) or 0
        log.info("✅ Supabase now has %d programs in the %dh window.", cnt, WINDOW_HOURS)
    except Exception as e:
        log.warning("Count query failed: %s", e)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    refresh_mv(sb)

# ==================== Entrypoint ====================

def main() -> int:
    log.info("PR pilot: iptv-org LIVE + Open-EPG Puerto Rico, window=%dh", WINDOW_HOURS)
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_pr(sb)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
