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

COUNTRY = os.environ.get("COUNTRY", "PR").upper()

# Windowing OFF by default (0 = no window filter)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "0"))

# Live gating OFF by default. Turn ON with ENFORCE_LIVE=1 when ready.
ENFORCE_LIVE = os.environ.get("ENFORCE_LIVE", "0") not in ("0", "false", "False", "")

# NEW: Skip rows with empty/junk titles (ON by default per your request)
SKIP_EMPTY_TITLES = os.environ.get("SKIP_EMPTY_TITLES", "1") not in ("0","false","False","")

# Naive times default offset (used only if a datetime has no tz info)
DEFAULT_NAIVE_TZ = os.environ.get("DEFAULT_NAIVE_TZ", "-0400")  # PR is UTC-4

# Open-EPG Puerto Rico candidates + discovery page
OPEN_EPG_INDEX = "https://www.open-epg.com/app/epgguide.php"
OPEN_EPG_CANDIDATES = [
    "https://www.open-epg.com/files/puertorico1.xml.gz",
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml.gz",
    "https://www.open-epg.com/files/puertorico2.xml",
]

# iptv-org public API
IPTV_CHANNELS_URL = "https://iptv-org.github.io/api/channels.json"
IPTV_STREAMS_URL  = "https://iptv-org.github.io/api/streams.json"

# Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# Performance / retries
REQUEST_TIMEOUT = (10, 180)
BATCH_CHANNELS = 2000
BATCH_PROGRAMS = 1000
MAX_RETRIES = 4

# MV refresh (harmless even if windowing is off)
REFRESH_MV_FUNC = os.environ.get("REFRESH_MV_FUNC", "refresh_programs_next_12h")
REFRESH_MV = os.environ.get("REFRESH_MV", "1") not in ("0", "false", "False", "")

# Debug
DEBUG_CHANNEL_SAMPLES = int(os.environ.get("DEBUG_CHANNEL_SAMPLES", "12"))
DEBUG_PROGRAM_SAMPLES = int(os.environ.get("DEBUG_PROGRAM_SAMPLES", "12"))
DEBUG_LOG_UNPARSEABLE = int(os.environ.get("DEBUG_LOG_UNPARSEABLE", "8"))

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

# ---------- Robust datetime parsing (PATCHED) ----------

_DT_PATTERNS = (
    # compact, seconds
    ("%Y%m%d%H%M%S%z", None),      # 20250822100000+0000 (after normalization)
    ("%Y%m%d%H%M%S",  "compact"),  # 20250822100000        (naive)

    # compact, minutes (some guides omit seconds)
    ("%Y%m%d%H%M%z",  None),       # 202508221000+0000
    ("%Y%m%d%H%M",    "compact"),  # 202508221000          (naive)

    # dashed, seconds
    ("%Y-%m-%d %H:%M:%S%z", None), # 2025-08-22 10:00:00+0000
    ("%Y-%m-%d %H:%M:%S",  "dashed"),

    # dashed, minutes
    ("%Y-%m-%d %H:%M%z",   None),  # 2025-08-22 10:00+0000
    ("%Y-%m-%d %H:%M",     "dashed"),
)

def _normalize_tz_tail(s: str) -> str:
    """
    Normalize various TZ tails:
      - 'Z' / 'z'         -> '+0000'
      - ' +HH:MM'         -> '+HHMM'
      - ' +HHMM'          -> '+HHMM'
      - remove any stray space before the tz
    Leaves strings without tz untouched.
    """
    s = s.strip()

    # trailing Z/z
    if s.endswith("Z") or s.endswith("z"):
        return s[:-1] + "+0000"

    # match optional space + tz like '+HH:MM' or '+HHMM'
    m = re.match(r"^(.*?)(?:\s+)?([+-]\d{2})(:?)(\d{2})$", s)
    if m:
        core, hh, colon, mm = m.groups()
        return f"{core}{hh}{mm}"

    return s

def parse_xmltv_datetime(raw: Optional[str], naive_tz: str = DEFAULT_NAIVE_TZ) -> Optional[datetime]:
    """Parse many common XMLTV datetime flavors; attach `naive_tz` if no tz present."""
    if not raw:
        return None
    s = _normalize_tz_tail(raw)

    for fmt, kind in _DT_PATTERNS:
        try:
            dt = datetime.strptime(s, fmt)
            if kind is None:
                return dt.astimezone(timezone.utc)
            # naive -> assume `naive_tz`
            m = re.fullmatch(r"([+-])(\d{2})(\d{2})", naive_tz.strip())
            if not m:
                return dt.replace(tzinfo=timezone.utc)
            sign, hh, mm = m.groups()
            offset = int(hh) * 60 + int(mm)
            if sign == "-":
                offset = -offset
            tz = timezone(timedelta(minutes=offset))
            return dt.replace(tzinfo=tz).astimezone(timezone.utc)
        except Exception:
            continue

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

# ---------- Name normalization (for live matching & logs) ----------

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

# ==================== iptv-org (build name index & live ids) ====================

def fetch_json(url: str):
    log.info("Fetching JSON: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def build_live_country_index(country_code: str) -> Tuple[Dict[str, Set[str]], Set[str]]:
    """Return (name_index, live_ids) for iptv-org COUNTRY."""
    channels = fetch_json(IPTV_CHANNELS_URL)
    streams  = fetch_json(IPTV_STREAMS_URL)
    cc = country_code.upper()

    country_ids = {ch["id"] for ch in channels if (ch.get("country") or "").upper() == cc}
    live_ids = {s.get("channel") for s in streams if s.get("channel") in country_ids}

    name_index: Dict[str, Set[str]] = {}
    def add_name(nm: str, cid: str):
        key = norm_name(nm)
        if not key: return
        name_index.setdefault(key, set()).add(cid)

    for ch in channels:
        cid = ch.get("id")
        if cid in country_ids:
            add_name(ch.get("name") or "", cid)
            for alt in ch.get("alt_names") or []:
                add_name(alt, cid)
    for st in streams:
        cid = st.get("channel")
        if cid in country_ids:
            add_name(st.get("title") or "", cid)

    log.info("iptv-org %s: total_ids=%d, live_ids=%d, name_keys=%d", cc, len(country_ids), len(live_ids), len(name_index))
    return name_index, live_ids

# ==================== Open-EPG PR discovery ====================

def discover_open_epg_pr() -> List[str]:
    urls: List[str] = []
    session = requests.Session()
    # Try known candidates
    for u in OPEN_EPG_CANDIDATES:
        try:
            r = session.head(u, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                urls.append(u)
        except Exception:
            pass
    # Scrape guide page
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
        log.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set."); sys.exit(1)
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
            # de-dup within batch
            dedup: Dict[str, dict] = {}
            for r in batch:
                k = r.get("id")
                if not k: continue
                dedup[k] = r
            batch = list(dedup.values())
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch); break
            except APIError as e:
                msg = str(e)
                need_split = any(s in msg for s in ("21000", "duplicate key value violates", "500", "413", "Payload"))
                if need_split and len(batch) > 1:
                    mid = len(batch)//2
                    queue.insert(0, batch[mid:]); queue.insert(0, batch[:mid])
                    log.warning("Splitting %s batch (%d) due to error: %s", table, len(batch), msg); break
                if attempt == MAX_RETRIES:
                    log.error("Giving up on %s batch (%d): %s", table, len(batch), msg)
                else:
                    sleep = attempt * rand_jitter()
                    log.warning("Retry %d/%d for %s (%d rows) in %.2fs: %s",
                                attempt, MAX_RETRIES, table, len(batch), sleep, msg)
                    time.sleep(sleep)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    sleep = attempt * rand_jitter()
                    log.warning("Retry %d/%d for %s (%d rows) in %.2fs (unexpected): %s",
                                attempt, MAX_RETRIES, table, len(batch), sleep, e)
                    time.sleep(sleep)
    log.info("Upserted %d rows into %s.", total, table)

def refresh_mv(sb: Client):
    if not REFRESH_MV:
        log.info("Skipping MV refresh (REFRESH_MV disabled)."); return
    try:
        log.info("Refreshing MV via RPC: %s …", REFRESH_MV_FUNC)
        sb.rpc(REFRESH_MV_FUNC).execute()
        log.info("✅ MV refreshed.")
    except Exception as e:
        log.warning("MV refresh failed: %s", e)

# ==================== Core ingest ====================

def fetch_and_process(sb: Client):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS) if WINDOW_HOURS > 0 else None
    log.info("Windowing: %s (naive times => %s)",
             ("OFF" if WINDOW_HOURS <= 0 else f"now→+{WINDOW_HOURS}h"), DEFAULT_NAIVE_TZ)

    # iptv-org index (for live gating and logs)
    name_index, live_ids = build_live_country_index(COUNTRY)

    epg_urls = discover_open_epg_pr()
    if not epg_urls:
        log.warning("No Open-EPG Puerto Rico files found; aborting."); return

    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}

    # Debug accumulators
    sample_channels: List[str] = []
    sample_programs: List[str]  = []
    unparseable = 0
    would_match_live = 0
    kept_live_channels = 0
    skipped_not_live = 0
    total_channels_seen = 0

    def is_live_match(names: List[str], ch_id_text: str) -> bool:
        # check normalized display-name(s) and channel id text
        for nm in names + [ch_id_text]:
            key = norm_name(nm)
            if key in name_index:
                return True
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

                c_seen = p_seen = p_kept = 0

                keep_channel: Dict[str, bool] = {}
                ch_names: Dict[str, List[str]] = {}
                ch_icon: Dict[str, Optional[str]] = {}

                for ev, el in context:
                    if ev != "end": continue
                    tag = localname(el.tag)

                    if tag == "channel":
                        c_seen += 1; total_channels_seen += 1
                        ch_id = el.get("id") or ""

                        # gather display-names if present (PR files usually don't have them)
                        names = []
                        for child in list(el):
                            if localname(child.tag).lower() == "display-name":
                                t = text_from(child)
                                if t: names.append(t)
                        # fallback to use the channel id text as a name
                        if not names and ch_id:
                            names = [ch_id]

                        ch_names[ch_id] = names
                        ch_icon[ch_id]  = icon_src(el)

                        matched = is_live_match(names, ch_id)
                        if matched: would_match_live += 1

                        # enforce live only if enabled
                        keep = (matched or not ENFORCE_LIVE)
                        keep_channel[ch_id] = keep
                        if keep:
                            disp = names[0] if names else ch_id
                            channels[ch_id] = {"id": ch_id, "display_name": disp, "icon_url": ch_icon[ch_id]}
                            kept_live_channels += 1 if matched else 0
                        else:
                            skipped_not_live += 1

                        if len(sample_channels) < DEBUG_CHANNEL_SAMPLES:
                            sample_channels.append(f"id={ch_id} names={names[:2]} match_live={matched} kept={keep}")

                        el.clear(); continue

                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel") or ""
                        if not keep_channel.get(ch_id, False):
                            el.clear(); continue

                        raw_start = el.get("start")
                        raw_stop  = el.get("stop")
                        s = parse_xmltv_datetime(raw_start)
                        e = parse_xmltv_datetime(raw_stop)
                        if not (s and e):
                            if unparseable < DEBUG_LOG_UNPARSEABLE:
                                log.info("DEBUG unparseable time: ch=%s start=%s stop=%s", ch_id, raw_start, raw_stop)
                            unparseable += 1
                            el.clear(); continue

                        # Windowing (optional)
                        if WINDOW_HOURS > 0:
                            if not (s <= horizon_utc and e >= now_utc):
                                el.clear(); continue

                        # Extract title/desc
                        title = ""
                        desc  = ""
                        for child in list(el):
                            nm = localname(child.tag).lower()
                            if nm == "title" and not title:
                                t = text_from(child).strip(); title = t
                            elif nm == "sub-title" and not title:
                                t = text_from(child).strip(); title = t
                            elif nm == "desc" and not desc:
                                d = text_from(child).strip(); desc = d

                        # === PATCH: drop empties if enabled ===
                        if SKIP_EMPTY_TITLES and not (title and title.strip()):
                            el.clear(); continue

                        pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                        row = {
                            "id": pid,
                            "channel_id": ch_id,
                            "start_time": s.isoformat(),
                            "end_time": e.isoformat(),
                            "title": (title or None),
                            "description": (desc or None)
                        }
                        if pid not in programs:
                            programs[pid] = row
                            p_kept += 1
                            if len(sample_programs) < DEBUG_PROGRAM_SAMPLES:
                                sample_programs.append(
                                    f"ch={ch_id} start={s.isoformat()} title={(title or '')[:60]!r} desc_len={len(desc)}"
                                )

                        el.clear()
                        if (p_kept % 8000) == 0:
                            root.clear()
                        continue

                    el.clear()

                log.info("Parsed %s: channels(seen)=%d, programs_found=%d, programs_kept=%d",
                         url, c_seen, p_seen, p_kept)

        except Exception as e:
            log.warning("Failed %s: %s", url, e)

    # Log debug samples
    if sample_channels:
        log.info("SAMPLE channels (%d of ~%d):\n  %s", len(sample_channels), total_channels_seen, "\n  ".join(sample_channels))
    if sample_programs:
        log.info("SAMPLE programmes (%d):\n  %s", len(sample_programs), "\n  ".join(sample_programs))

    log.info("Unparseable time entries: %d", unparseable)
    log.info("EPG channels matching iptv-org live set (PR): %d  (kept_live=%d, skipped_not_live=%d; ENFORCE_LIVE=%s)",
             would_match_live, kept_live_channels, skipped_not_live, ENFORCE_LIVE)

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
        log.warning("No programmes parsed (likely all empty titles and SKIP_EMPTY_TITLES=1).")

    # Verify, cleanup (only if windowing on), MV refresh
    try:
        if WINDOW_HOURS > 0:
            end_utc = now_utc + timedelta(hours=WINDOW_HOURS)
            res = sb.table("programs").select("id", count="exact")\
                .gte("end_time", now_utc.isoformat())\
                .lte("start_time", end_utc.isoformat())\
                .execute()
            cnt = getattr(res, "count", None) or 0
            log.info("✅ Supabase now has %d programs in the %dh window.", cnt, WINDOW_HOURS)
        else:
            res = sb.table("programs").select("id", count="exact").execute()
            cnt = getattr(res, "count", None) or 0
            log.info("✅ Supabase now has %d total programs (no window).", cnt)
    except Exception as e:
        log.warning("Count query failed: %s", e)

    if WINDOW_HOURS > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS)
        try:
            sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
            log.info("Cleaned up programs with end_time < %s", cutoff.isoformat())
        except Exception as e:
            log.warning("Cleanup failed: %s", e)

    if REFRESH_MV:
        refresh_mv(sb)

# ==================== Entrypoint ====================

def main() -> int:
    log.info("PR ingest (Open-EPG). WINDOW_HOURS=%d, ENFORCE_LIVE=%s, SKIP_EMPTY_TITLES=%s",
             WINDOW_HOURS, ENFORCE_LIVE, SKIP_EMPTY_TITLES)
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process(sb)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
