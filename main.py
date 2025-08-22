#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, gzip, time, logging, itertools, random, re, unicodedata
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Set
import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ======== Config ========
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "12"))
COUNTRY = "PR"  # Puerto Rico pilot
FILTER_LIVE = True           # require at least one stream in iptv-org for PR
REFRESH_MV_FUNC = os.environ.get("REFRESH_MV_FUNC", "refresh_programs_next_12h")
REQUEST_TIMEOUT = (10, 180)
BATCH_CHANNELS = 2000
BATCH_PROGRAMS = 1000
MAX_RETRIES = 4

# Open-EPG discovery targets for Puerto Rico
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg-pr")

# ======== Utils ========
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

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw: return None
    s = raw.strip()
    if " " in s:
        a, b = s.rsplit(" ", 1); s = a + b        # remove space before tz
    if len(s) >= 6 and (not s.endswith("Z")) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]                       # +HH:MM -> +HHMM
    if s.endswith("Z"): s = s[:-1] + "+0000"
    if len(s) == 14: s += "+0000"
    try:
        return datetime.strptime(s, "%Y%m%d%H%M%S%z").astimezone(timezone.utc)
    except Exception:
        return None

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

# Name normalization
STOP = {"tv","hd","uhd","sd","channel","ch","the","pr","us","puerto","rico","+1","+2","fhd","4k"}
TOKEN_RE = re.compile(r"[^\w]+")

def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = s.replace("&"," and ")
    s = " ".join(s.split())
    return s

def tokens(s: str) -> Set[str]:
    return {t for t in TOKEN_RE.split(norm_name(s)) if t and t not in STOP and len(t) > 1}

JUNK_TITLES = {"", "title", "no title", "untitled", "-", "programme", "program"}

# ======== iptv-org live set (PR) ========
def fetch_json(url: str):
    log.info("Fetching JSON: %s", url)
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def build_live_pr():
    """Return (live_name_tokens, name_variants, live_ids)
       - live_name_tokens: set of tokenized, normalized names for matching
       - name_variants: map normalized name -> set(channel_ids)
       - live_ids: set of channel ids (iptv-org) that have at least one stream and country=PR
    """
    channels = fetch_json(IPTV_CHANNELS_URL)  # has 'id','name','alt_names','country',...
    streams  = fetch_json(IPTV_STREAMS_URL)   # has 'channel','url','title',...
    pr_ids = {ch["id"] for ch in channels if (ch.get("country") or "").upper() == "PR"}
    live_ids = {s.get("channel") for s in streams if s.get("channel") in pr_ids}

    name_variants: Dict[str, Set[str]] = {}
    live_name_tokens: Set[str] = set()

    # Helper: add a name -> channel id mapping
    def add_name(nm: str, cid: str):
        key = norm_name(nm)
        if not key: return
        name_variants.setdefault(key, set()).add(cid)

    for ch in channels:
        cid = ch.get("id")
        if cid not in live_ids: continue
        add_name(ch.get("name") or "", cid)
        for alt in ch.get("alt_names") or []:
            add_name(alt, cid)

    # also learn from stream titles (helpful aliases)
    for st in streams:
        cid = st.get("channel")
        if cid in live_ids:
            add_name(st.get("title") or "", cid)

    # token cache
    for key in name_variants.keys():
        for t in tokens(key):
            live_name_tokens.add(t)

    log.info("iptv-org PR: live_ids=%d, name_keys=%d, token_vocab=%d", len(live_ids), len(name_variants), len(live_name_tokens))
    return live_name_tokens, name_variants, live_ids

# ======== discover Open-EPG Puerto Rico URLs ========
def discover_open_epg_pr() -> List[str]:
    urls: List[str] = []
    # 1) try known candidates
    session = requests.Session()
    for u in OPEN_EPG_CANDIDATES:
        try:
            r = session.head(u, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                urls.append(u)
        except Exception:
            pass
    # 2) scan the guide page for /files/puertorico*.xml(.gz)
    try:
        r = session.get(OPEN_EPG_INDEX, timeout=REQUEST_TIMEOUT)
        if r.ok:
            m = re.findall(r"https://www\.open-epg\.com/files/puertorico\d+\.xml(?:\.gz)?", r.text, flags=re.I)
            for u in m:
                if u not in urls:
                    urls.append(u)
    except Exception:
        pass
    urls = list(dict.fromkeys(urls))  # de-dup, preserve order
    log.info("Open-EPG PR URLs discovered: %s", ", ".join(urls) if urls else "(none)")
    return urls

# ======== Supabase ========
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
            # dedupe within-batch preferring non-junk/longer desc
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

def refresh_mv(sb: Client):
    try:
        log.info("Refreshing MV via RPC: %s …", REFRESH_MV_FUNC)
        sb.rpc(REFRESH_MV_FUNC).execute()
        log.info("✅ MV refreshed.")
    except Exception as e:
        log.warning("MV refresh failed: %s", e)

# ======== Core ingest (Puerto Rico only) ========
def fetch_and_process_pr(sb: Client):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
    log.info("Window: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())

    live_tokens, name_index, live_ids = build_live_pr()
    if FILTER_LIVE and not live_ids:
        log.warning("No live PR ids found in iptv-org; aborting.")
        return

    epg_urls = discover_open_epg_pr()
    if not epg_urls:
        log.warning("No Open-EPG Puerto Rico files found; aborting.")
        return

    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}

    def is_live_match(names: List[str]) -> bool:
        # direct normalized name -> any live channel id
        for nm in names:
            key = norm_name(nm)
            if key in name_index:
                return True
            # token match fallback (strong)
            tk = tokens(key)
            if tk and any(t in live_tokens for t in tk):
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
                            keep = is_live_match(names) or is_live_match([ch_id])
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

                        s = parse_xmltv_datetime(el.get("start"))
                        e = parse_xmltv_datetime(el.get("stop"))
                        if not (s and e): el.clear(); continue
                        if not (s <= horizon_utc and e >= now_utc): el.clear(); continue

                        # choose best non-junk title; prefer first <title> then <sub-title>
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
    cnt = -1
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

# ======== Entrypoint ========
def main() -> int:
    log.info("PR pilot: iptv-org LIVE + Open-EPG Puerto Rico, window=%dh", WINDOW_HOURS)
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_pr(sb)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
