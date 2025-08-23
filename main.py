#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, time, logging, itertools
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional, Tuple

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

OPEN_EPG_URLS = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]

REQUEST_TIMEOUT = (10, 180)  # (connect, read)
BATCH_CHANNELS = 1000
BATCH_PROGRAMS = 1000
MAX_RETRIES = 4

# Window 0 == OFF (ingest everything)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "0"))

# Debug: look up a specific slot (optional)
TEST_LOOKUP_CHANNEL = os.environ.get("TEST_LOOKUP_CHANNEL", "")
TEST_LOOKUP_START   = os.environ.get("TEST_LOOKUP_START", "")

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

def localname(tag: str) -> str:
    if not tag:
        return tag
    return tag.split('}', 1)[-1] if tag[0] == '{' else tag

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
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

def first_texts(elem: ET.Element, want: str) -> List[Tuple[str,str]]:
    """Return list of (lang, text) for direct children with localname == want."""
    out: List[Tuple[str,str]] = []
    w = want.lower()
    for child in list(elem):
        if localname(child.tag).lower() == w:
            lang = (child.attrib.get("lang") or "").strip()
            txt = ''.join(child.itertext()).strip()
            out.append((lang, txt))
    return out

def pick_lang_text(candidates: List[Tuple[str,str]], prefer_langs: List[str]) -> str:
    # prefer first non-empty by preferred languages, then any non-empty
    for pl in prefer_langs:
        for lang, txt in candidates:
            if txt and (lang.lower() == pl.lower()):
                return txt
    for _, txt in candidates:
        if txt:
            return txt
    return ""

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
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch)
                break
            except APIError as e:
                msg = str(e)
                need_split = ("21000" in msg or
                              "duplicate key value violates" in msg or
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
                    time.sleep(0.5 * attempt)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    time.sleep(0.5 * attempt)
    log.info("Upserted %d rows into %s.", total, table)

# ----------------------- XML ingest -------------------

def fetch_xml(url: str) -> bytes:
    log.info("Fetching EPG (XML only): %s", url)
    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
        resp.raise_for_status()
        # read all; file is plain XML (no gzip here)
        return resp.content

def ingest_open_epg_xml(xml_bytes: bytes,
                        programs: Dict[str, dict],
                        channels: Dict[str, dict],
                        prefer_langs: List[str],
                        now_utc: Optional[datetime],
                        horizon_utc: Optional[datetime]) -> Tuple[int,int,float]:
    """
    Parse XML and append to `programs` & `channels`. Returns (ch_seen, p_seen, titled_ratio).
    IMPORTANT FIX: don't clear child nodes before reading <programme>.
    """
    context = ET.iterparse(io.BytesIO(xml_bytes), events=("end",))
    _, root = next(context)

    ch_seen = 0
    p_seen = 0
    nonempty_titles = 0

    # map channel id -> display name (collect once)
    # NOTE: we'll only clear nodes at 'channel' and 'programme' end events.
    for ev, el in context:
        tag = localname(el.tag)

        if tag == "channel":
            ch_id = el.attrib.get("id")
            if ch_id:
                # pick first display-name text, else id
                name = pick_lang_text(first_texts(el, "display-name"), prefer_langs) or ch_id
                icon = None
                # optional <icon src="...">
                for child in list(el):
                    if localname(child.tag).lower() == "icon":
                        for k, v in child.attrib.items():
                            if localname(k).lower() == "src" and v:
                                icon = v.strip()
                                break
                if ch_id not in channels:
                    channels[ch_id] = {"id": ch_id, "display_name": name, "icon_url": icon}
                    ch_seen += 1
            el.clear()
            continue

        if tag == "programme":
            p_seen += 1
            ch_id = el.attrib.get("channel")
            s_raw = el.attrib.get("start")
            e_raw = el.attrib.get("stop") or el.attrib.get("end")
            s = parse_xmltv_datetime(s_raw)
            e = parse_xmltv_datetime(e_raw)
            if not (ch_id and s and e):
                el.clear()
                continue

            # optional windowing
            if now_utc and horizon_utc:
                if not (s <= horizon_utc and e >= now_utc):
                    el.clear()
                    continue

            titles = first_texts(el, "title")
            descs  = first_texts(el, "desc")
            title  = pick_lang_text(titles, prefer_langs).strip()
            desc   = pick_lang_text(descs, prefer_langs).strip() or None

            if title:
                nonempty_titles += 1

            pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
            row = {
                "id": pid,
                "channel_id": ch_id,
                "start_time": s.isoformat(),
                "end_time": e.isoformat(),
                "title": title or None,          # keep NULL, not empty string
                "description": desc
            }
            # keep the "best" version if duplicate id is encountered
            prev = programs.get(pid)
            if prev is None:
                programs[pid] = row
            else:
                # prefer non-empty title and longer description
                prev_t = prev.get("title") or ""
                cand_t = row.get("title") or ""
                prev_d = prev.get("description") or ""
                cand_d = row.get("description") or ""
                replace = False
                if not prev_t and cand_t:
                    replace = True
                elif len(cand_d) > len(prev_d):
                    replace = True
                if replace:
                    programs[pid] = row

            el.clear()
            # free memory occasionally
            if (p_seen % 8000) == 0:
                root.clear()
            continue

        # ⚠️ DO NOT el.clear() for other child nodes here.
        # We must keep them intact until we process the parent <programme>.

    titled_ratio = (nonempty_titles / p_seen) if p_seen else 0.0
    return ch_seen, p_seen, titled_ratio

# ----------------------- Core ingest ------------------

def main() -> int:
    prefer_langs = [s for s in (os.environ.get("PREFER_LANGS", "es-pr,es,en").split(",")) if s]
    window = int(os.environ.get("WINDOW_HOURS", str(WINDOW_HOURS)))
    log.info("Open-EPG ingest (PR, XML only). WINDOW_HOURS=%d, ENFORCE_LIVE=False, SKIP_EMPTY_TITLES=False, PREFER_LANGS=%s",
             window, ",".join(prefer_langs))

    sb = init_supabase()

    now_utc = datetime.now(timezone.utc) if window > 0 else None
    horizon_utc = (now_utc + timedelta(hours=window)) if now_utc else None
    log.info("Windowing: %s", "OFF" if not now_utc else f"{now_utc.isoformat()} -> {horizon_utc.isoformat()}")

    channels: Dict[str, dict] = {}
    programs: Dict[str, dict] = {}

    total_ch_seen = 0
    total_p_seen = 0
    total_nonempty_ratio_nums = 0.0
    total_nonempty_ratio_dens = 0

    for url in OPEN_EPG_URLS:
        try:
            xml = fetch_xml(url)
            ch_seen, p_seen, titled_ratio = ingest_open_epg_xml(
                xml, programs, channels, prefer_langs, now_utc, horizon_utc
            )
            total_ch_seen += ch_seen
            total_p_seen += p_seen
            total_nonempty_ratio_nums += titled_ratio * p_seen
            total_nonempty_ratio_dens += p_seen
            log.info("Parsed %s: channels(seen)=%d, programs_found=%d, kept=%d, titled_ratio=%.3f",
                     url, ch_seen, p_seen, p_seen, titled_ratio)
        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

    # Debug: sample a few items that look empty
    empties = []
    for r in programs.values():
        if not (r.get("title") or r.get("description")):
            empties.append(r)
            if len(empties) == 6:
                break
    if empties:
        log.info("SAMPLE EMPTY-looking programmes (6):")
        for r in empties:
            log.info("  ch=%s start=%s", r["channel_id"], r["start_time"])

    # Optional test lookup
    if TEST_LOOKUP_CHANNEL and TEST_LOOKUP_START:
        # normalize start
        sdt = parse_xmltv_datetime(TEST_LOOKUP_START)
        title = desc = None
        if sdt:
            key_prefix = f"{TEST_LOOKUP_CHANNEL}_{sdt.strftime('%Y%m%d%H%M%S')}_"
            for pid, row in programs.items():
                if pid.startswith(key_prefix):
                    title = row.get("title")
                    desc = row.get("description") or ""
                    break
        log.info("TEST LOOKUP: %s @ %s -> title=%s, desc_len=%d",
                 TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START, repr(title), len(desc or ""))

    # Upserts
    if channels:
        log.info("Upserting %d channels …", len(channels))
        upsert_with_retry(sb, "channels", list(channels.values()), conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels to upsert.")

    prog_rows = list(programs.values())
    log.info("Programs to upsert (deduped): %d", len(prog_rows))

    if prog_rows:
        prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programmes parsed (check parser/window).")

    # Quick DB sanity: how many rows have non-null title/description?
    try:
        total = sb.table("programs").select("id").execute()
        with_title = sb.table("programs").select("id").not_.is_("title", "null").execute()
        with_desc = sb.table("programs").select("id").not_.is_("description", "null").execute()
        log.info("DB sanity: total=%d, with_title=%d, with_desc=%d",
                 len(getattr(total, "data", []) or []),
                 len(getattr(with_title, "data", []) or []),
                 len(getattr(with_desc, "data", []) or []))
    except Exception as e:
        log.warning("DB sanity check failed: %s", e)

    # Refresh MV (12h name in your DB)
    try:
        log.info("Refreshing MV via RPC: refresh_programs_next_12h …")
        sb.rpc("refresh_programs_next_12h").execute()
        log.info("✅ MV refreshed.")
    except Exception as e:
        log.warning("MV refresh failed: %s", e)

    log.info("Done. Channels upserted: %d; Programs considered: %d",
             len(channels), len(prog_rows))
    return 0

if __name__ == "__main__":
    try:
        import io  # ensure available for iterparse stream
    except Exception:
        pass
    sys.exit(main())
