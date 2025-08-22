#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, time, gzip, logging, itertools, random, re
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Tuple

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ====================== Config ======================

# We’re validating text extraction, so keep things simple:
COUNTRY = os.environ.get("COUNTRY", "PR").upper()

# Only XML, no .gz (you can override via env if needed)
OPEN_EPG_FILES = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]
_env_files = os.environ.get("OPEN_EPG_FILES")
if _env_files:
    # allow comma-separated override (but we’ll still ignore .gz on purpose)
    OPEN_EPG_FILES = [u.strip() for u in _env_files.split(",") if u.strip() and not u.strip().endswith(".gz")]

# Window OFF for now (0 = no time filter)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "0"))

# Live gating OFF for now (we’re testing extraction first)
ENFORCE_LIVE = os.environ.get("ENFORCE_LIVE", "0") not in ("0","false","False","")

# If you want to drop rows with empty title (after fallbacks), flip this on later
SKIP_EMPTY_TITLES = os.environ.get("SKIP_EMPTY_TITLES", "0") not in ("0","false","False","")

# Prefer these languages when choosing title/desc text
PREFER_LANGS: Tuple[str, ...] = tuple(
    x.strip().lower() for x in os.environ.get("PREFER_LANGS", "es-pr,es,en").split(",")
)

# If a parsed file has very few titled programmes, you can auto-skip it by setting this > 0
# e.g. RICHNESS_MIN_TITLE_RATIO=0.02  (2%). Default 0.0 means “do not gate/skip”.
RICHNESS_MIN_TITLE_RATIO = float(os.environ.get("RICHNESS_MIN_TITLE_RATIO", "0.0"))

# Naive times default offset (only if datetime has no tz info)
DEFAULT_NAIVE_TZ = os.environ.get("DEFAULT_NAIVE_TZ", "-0400")  # PR: UTC-4

# iptv-org public API (kept for live gating later; unused when ENFORCE_LIVE=False)
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

# MV refresh (kept; harmless even with windowing off)
REFRESH_MV_FUNC = os.environ.get("REFRESH_MV_FUNC", "refresh_programs_next_12h")
REFRESH_MV = os.environ.get("REFRESH_MV", "1") not in ("0","false","False","")

# Debug knobs
DEBUG_CHANNEL_SAMPLES = int(os.environ.get("DEBUG_CHANNEL_SAMPLES", "10"))
DEBUG_PROGRAM_SAMPLES = int(os.environ.get("DEBUG_PROGRAM_SAMPLES", "10"))
DEBUG_DUMP_PROGRAM_CHILDREN = int(os.environ.get("DEBUG_DUMP_PROGRAM_CHILDREN", "0"))  # set >0 to see child nodes
DEBUG_FOCUS_CHANNEL_IDS = [x.strip() for x in os.environ.get("DEBUG_FOCUS_CHANNEL_IDS", "").split(",") if x.strip()]

# One-shot test: log the title/desc for a specific programme (channel id + start stamp)
# Example:
#   TEST_LOOKUP_CHANNEL="3ABN LATINO WTPM DT3 PUERTO RICO.pr"
#   TEST_LOOKUP_START="20250822223000 +0000"
TEST_LOOKUP_CHANNEL = os.environ.get("TEST_LOOKUP_CHANNEL", "").strip()
TEST_LOOKUP_START   = os.environ.get("TEST_LOOKUP_START", "").strip()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-epg-xml")

# ================== Generic helpers ==================

def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, size))
        if not block: return
        yield block

def rand_jitter() -> float:
    return 0.25 + random.random() * 0.75

def open_xml_stream(resp: requests.Response):
    # Only XML endpoints here; keep gzip check harmlessly in case server gzips
    resp.raw.decode_content = True
    ct = (resp.headers.get("Content-Type") or "").lower()
    gz = ("gzip" in ct)
    return gzip.GzipFile(fileobj=resp.raw) if gz else resp.raw

# -------------- Robust datetime parsing --------------

_DT_PATTERNS = (
    ("%Y%m%d%H%M%S%z", None),      # 20250822100000+0000
    ("%Y%m%d%H%M%S",  "compact"),  # 20250822100000
    ("%Y%m%d%H%M%z",  None),       # 202508221000+0000
    ("%Y%m%d%H%M",    "compact"),  # 202508221000
    ("%Y-%m-%d %H:%M:%S%z", None),
    ("%Y-%m-%d %H:%M:%S",  "dashed"),
    ("%Y-%m-%d %H:%M%z",   None),
    ("%Y-%m-%d %H:%M",     "dashed"),
)

def _normalize_tz_tail(s: str) -> str:
    s = s.strip()
    if s.endswith(("Z","z")): return s[:-1] + "+0000"
    m = re.match(r"^(.*?)(?:\s+)?([+-]\d{2})(:?)(\d{2})$", s)
    if m:
        core, hh, _, mm = m.groups()
        return f"{core}{hh}{mm}"
    return s

def parse_xmltv_datetime(raw: Optional[str], naive_tz: str = DEFAULT_NAIVE_TZ) -> Optional[datetime]:
    if not raw: return None
    s = _normalize_tz_tail(raw)
    for fmt, kind in _DT_PATTERNS:
        try:
            dt = datetime.strptime(s, fmt)
            if kind is None:
                return dt.astimezone(timezone.utc)
            # naive -> use DEFAULT_NAIVE_TZ
            m = re.fullmatch(r"([+-])(\d{2})(\d{2})", naive_tz.strip())
            if not m:
                return dt.replace(tzinfo=timezone.utc)
            sign, hh, mm = m.groups()
            offset = int(hh) * 60 + int(mm)
            if sign == "-": offset = -offset
            tz = timezone(timedelta(minutes=offset))
            return dt.replace(tzinfo=tz).astimezone(timezone.utc)
        except Exception:
            continue
    return None

# -------------------- XML helpers --------------------

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

def collect_program_children(el: ET.Element) -> List[Tuple[str,str,str]]:
    """
    Return [(tag, lang, text_or_value)] for child elements of <programme>.
    Maintains document order so we can pick the LAST non-empty node.
    """
    items: List[Tuple[str,str,str]] = []
    for child in list(el):
        tag = localname(child.tag).lower()
        lang = (child.attrib.get("lang") or "").strip().lower()
        txt = text_from(child)
        if not txt:
            v = child.attrib.get("value")
            if v: txt = (v or "").strip()
        items.append((tag, lang, (txt or "")))
    return items

def choose_last_nonempty(items: List[Tuple[str,str,str]], want_tag: str, prefer_langs: Tuple[str,...]) -> str:
    """
    Choose LAST non-empty occurrence of want_tag, honoring language prefs.
    Matches your observation: sheet’s later columns (e.g., L) carry the real payload.
    """
    cands = [(idx, lang, txt) for idx,(tag,lang,txt) in enumerate(items) if tag == want_tag and (txt or "").strip()]
    if not cands: return ""
    pref = [(i,l,t) for (i,l,t) in cands if l in prefer_langs]
    if pref:
        return pref[-1][2]   # last among preferred langs
    return cands[-1][2]      # last overall

def extract_title_desc(program_el: ET.Element) -> Tuple[str, str, List[Tuple[str,str,str]]]:
    items = collect_program_children(program_el)

    # Primary: children (LAST non-empty)
    title = choose_last_nonempty(items, "title", PREFER_LANGS)
    if not title:
        st = choose_last_nonempty(items, "sub-title", PREFER_LANGS)
        if st: title = st
    desc = choose_last_nonempty(items, "desc", PREFER_LANGS)

    # Attr fallback
    if not title:
        for k in ("title","name"):
            v = (program_el.attrib.get(k) or "").strip()
            if v: title = v; break
    if not desc:
        for k in ("desc","description","summary","synopsis"):
            v = (program_el.attrib.get(k) or "").strip()
            if v: desc = v; break

    # Any other child text as desc fallback
    if not desc:
        for tag, lang, txt in items:
            if tag not in ("title","sub-title","desc") and (txt or "").strip():
                desc = txt; break

    # Final promotion: if still no title but have desc → title = first line of desc
    if (not title) and (desc or "").strip():
        first = desc.splitlines()[0].strip()
        if first: title = first[:140]

    return (title or ""), (desc or ""), items

# ===================== iptv-org (optional) =====================

def fetch_json(url: str):
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def build_live_country_index(country_code: str) -> Tuple[Dict[str, set], set]:
    # Not used when ENFORCE_LIVE=False; kept for later
    try:
        channels = fetch_json(IPTV_CHANNELS_URL)
        streams  = fetch_json(IPTV_STREAMS_URL)
    except Exception:
        return {}, set()
    cc = country_code.upper()
    country_ids = {ch.get("id") for ch in channels if (ch.get("country") or "").upper() == cc}
    live_ids = {s.get("channel") for s in streams if s.get("channel") in country_ids}
    name_index: Dict[str, set] = {}
    def add_name(nm: str, cid: str):
        key = (nm or "").strip().lower()
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
    return name_index, live_ids

# ====================== Supabase ======================

def init_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set."); sys.exit(1)
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("✅ Connected to Supabase."); return sb
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
                dedup[k] = r
            batch = list(dedup.values())
        for attempt in range(1, MAX_RETRIES+1):
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
    if not REFRESH_MV: return
    try:
        sb.rpc(REFRESH_MV_FUNC).execute()
        log.info("✅ MV refreshed.")
    except Exception as e:
        log.warning("MV refresh failed: %s", e)

# ====================== Core ingest ======================

def fetch_and_process(sb: Client):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS) if WINDOW_HOURS > 0 else None
    log.info("Windowing: %s", ("OFF" if WINDOW_HOURS <= 0 else f"now→+{WINDOW_HOURS}h"))

    # Optional live index (kept for later)
    name_index, live_ids = build_live_country_index(COUNTRY) if ENFORCE_LIVE else ({}, set())

    # Global containers
    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}  # pid -> row (best-of across files)

    for url in OPEN_EPG_FILES:
        log.info("Fetching EPG (XML only): %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status_code == 404:
                    log.warning("404 for %s, skipping", url); continue
                resp.raise_for_status()
                stream = open_xml_stream(resp)
                context = ET.iterparse(stream, events=("start","end"))
                _, root = next(context)

                # local collectors for this file (so we can gate/skip if “thin”)
                local_channels: Dict[str, dict] = {}
                local_programs: Dict[str, dict] = {}

                c_seen = p_seen = p_kept = good_titles = 0
                dumped_children = 0

                for ev, el in context:
                    if ev != "end": continue
                    tag = localname(el.tag)

                    if tag == "channel":
                        c_seen += 1
                        ch_id = el.get("id") or ""
                        # best display name
                        disp = None
                        for child in list(el):
                            if localname(child.tag).lower() == "display-name":
                                t = text_from(child).strip()
                                if t and not disp:
                                    disp = t
                        if not disp: disp = ch_id or "Unknown"
                        if ch_id and ch_id not in local_channels:
                            local_channels[ch_id] = {"id": ch_id, "display_name": disp, "icon_url": icon_src(el)}
                        el.clear(); continue

                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel") or ""

                        raw_start = el.get("start"); raw_stop = el.get("stop")
                        s = parse_xmltv_datetime(raw_start); e = parse_xmltv_datetime(raw_stop)
                        if not (s and e):
                            el.clear(); continue

                        if WINDOW_HOURS > 0:
                            if not (s <= horizon_utc and e >= now_utc):
                                el.clear(); continue

                        title, desc, items = extract_title_desc(el)
                        if title.strip(): good_titles += 1

                        if DEBUG_DUMP_PROGRAM_CHILDREN and dumped_children < DEBUG_DUMP_PROGRAM_CHILDREN:
                            dumped_children += 1
                            lines = []
                            for tg, ln, tx in items:
                                shown = (tx[:80] + "…") if len(tx) > 80 else tx
                                lines.append(f"    <{tg} lang='{ln}'> {('∅' if not tx else shown)}")
                            log.info(
                                "DEBUG children ch=%s start=%s\n%s\n    -> chosen title=%r, desc_len=%d",
                                ch_id, s.isoformat(), "\n".join(lines) if lines else "    (no child nodes)",
                                (title or ""), len(desc or "")
                            )

                        if SKIP_EMPTY_TITLES and not title.strip():
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

                        old = local_programs.get(pid)
                        if old is None:
                            local_programs[pid] = row; p_kept += 1
                        else:
                            # prefer non-empty title; if tie, longer desc
                            old_t = (old.get("title") or "").strip()
                            new_t = (row.get("title") or "").strip()
                            old_d = (old.get("description") or "") or ""
                            new_d = (row.get("description") or "") or ""
                            replace = False
                            if not old_t and new_t: replace = True
                            elif (bool(new_t) == bool(old_t)) and (len(new_d) > len(old_d)):
                                replace = True
                            if replace: local_programs[pid] = row

                        el.clear()
                        if (p_kept % 8000) == 0:
                            root.clear()
                        continue

                    el.clear()

                # richness ratio for this file
                ratio = (good_titles / p_seen) if p_seen else 0.0
                log.info("Parsed %s: channels(seen)=%d, programs_found=%d, kept=%d, titled_ratio=%.3f",
                         url, c_seen, p_seen, p_kept, ratio)

                if RICHNESS_MIN_TITLE_RATIO > 0.0 and ratio < RICHNESS_MIN_TITLE_RATIO:
                    log.warning("Skipping %s due to low titled_ratio=%.3f < %.3f",
                                url, ratio, RICHNESS_MIN_TITLE_RATIO)
                    continue

                # merge local → global (best-of across files)
                for ch_id, ch_row in local_channels.items():
                    if ch_id not in channels: channels[ch_id] = ch_row
                for pid, new_row in local_programs.items():
                    old = programs.get(pid)
                    if old is None:
                        programs[pid] = new_row
                    else:
                        old_t = (old.get("title") or "").strip()
                        new_t = (new_row.get("title") or "").strip()
                        old_d = (old.get("description") or "") or ""
                        new_d = (new_row.get("description") or "") or ""
                        replace = False
                        if not old_t and new_t: replace = True
                        elif (bool(new_t) == bool(old_t)) and (len(new_d) > len(old_d)):
                            replace = True
                        if replace: programs[pid] = new_row

        except Exception as e:
            log.warning("Failed %s: %s", url, e)

    # ===== One-shot verification lookup (e.g., Salud Total @ 20250822223000 +0000)
    if TEST_LOOKUP_CHANNEL and TEST_LOOKUP_START:
        s = parse_xmltv_datetime(TEST_LOOKUP_START)
        if s:
            prefix = f"{TEST_LOOKUP_CHANNEL}_{s.strftime('%Y%m%d%H%M%S')}_"
            hits = [r for pid, r in programs.items() if pid.startswith(prefix)]
            if hits:
                r = sorted(hits, key=lambda x: x["end_time"])[0]
                log.info("TEST LOOKUP: %s @ %s -> title=%r, desc=%r",
                         TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START, r["title"], (r["description"] or "")[:240])
            else:
                log.info("TEST LOOKUP: no match for channel=%s start=%s", TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START)
        else:
            log.info("TEST LOOKUP: cannot parse TEST_LOOKUP_START=%r", TEST_LOOKUP_START)

    # ===== Ensure channels exist for referenced programmes
    referenced = {r["channel_id"] for r in programs.values()}
    for ch_id in referenced:
        if ch_id and ch_id not in channels:
            channels[ch_id] = {"id": ch_id, "display_name": ch_id, "icon_url": None}

    # ===== Upserts
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
        log.warning("No programmes parsed (check feeds).")

    # Verify counts
    try:
        if WINDOW_HOURS > 0:
            end_utc = datetime.now(timezone.utc) + timedelta(hours=WINDOW_HOURS)
            res = sb.table("programs").select("id", count="exact")\
                .gte("end_time", datetime.now(timezone.utc).isoformat())\
                .lte("start_time", end_utc.isoformat())\
                .execute()
            cnt = getattr(res, "count", None) or 0
            log.info("✅ Supabase now has %d programmes in the %dh window.", cnt, WINDOW_HOURS)
        else:
            res = sb.table("programs").select("id", count="exact").execute()
            cnt = getattr(res, "count", None) or 0
            log.info("✅ Supabase now has %d total programmes.", cnt)
    except Exception as e:
        log.warning("Count query failed: %s", e)

    if REFRESH_MV:
        refresh_mv(sb)

# ====================== Entrypoint ======================

def main() -> int:
    log.info("Open-EPG ingest (PR, XML only). WINDOW_HOURS=%d, ENFORCE_LIVE=%s, SKIP_EMPTY_TITLES=%s, PREFER_LANGS=%s",
             WINDOW_HOURS, ENFORCE_LIVE, SKIP_EMPTY_TITLES, ",".join(PREFER_LANGS))
    if any(u.lower().endswith(".gz") for u in OPEN_EPG_FILES):
        log.warning("You configured a .gz URL; this script intentionally ignores .gz to avoid 'thin' mirrors.")
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process(sb)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
