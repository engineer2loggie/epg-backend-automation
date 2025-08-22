#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, time, logging, itertools, io, re
from typing import Dict, List, Iterable, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from xml.etree import ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-epg")

# ----------------------- Config ------------------------

DEFAULT_EPG_URLS = [
    "https://www.open-epg.com/files/puertorico1.xml",
    "https://www.open-epg.com/files/puertorico2.xml",
]
_raw_urls = os.environ.get("OPEN_EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_EPG_URLS)

WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "0"))
SKIP_EMPTY_TITLES = os.environ.get("SKIP_EMPTY_TITLES", "0") in ("1", "true", "True")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

BATCH_CHANNELS = int(os.environ.get("BATCH_CHANNELS", "2000"))
BATCH_PROGRAMS = int(os.environ.get("BATCH_PROGRAMS", "1000"))
REFRESH_FUNC = os.environ.get("REFRESH_FUNC", "refresh_programs_next_12h")

MAX_RETRIES = 4
REQUEST_TIMEOUT = (10, 180)

TEST_LOOKUP_CHANNEL = os.environ.get("TEST_LOOKUP_CHANNEL")  # e.g. 3ABN LATINO WTPM DT3 PUERTO RICO.pr
TEST_LOOKUP_START   = os.environ.get("TEST_LOOKUP_START")    # e.g. 20250822223000 +0000

EMPTY_SAMPLES_TO_LOG  = int(os.environ.get("EMPTY_SAMPLES_TO_LOG", "6"))
FILLED_SAMPLES_TO_LOG = int(os.environ.get("FILLED_SAMPLES_TO_LOG", "6"))
RAW_EMPTY_SNIPPETS    = int(os.environ.get("RAW_EMPTY_SNIPPETS", "3"))  # dump a few raw <programme>s

# ----------------------- Helpers -----------------------

def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    it = iter(seq)
    while True:
        block = list(itertools.islice(it, size))
        if not block:
            return
        yield block

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    s = raw.strip()
    if " " in s:
        a, b = s.rsplit(" ", 1)
        if re.fullmatch(r"[+-]\d{4}", b):
            s = a + b
    if s.endswith("Z"):
        s = s[:-1] + "+0000"
    if len(s) >= 6 and (not s.endswith("Z")) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]
    if len(s) == 14 and s.isdigit():
        s += "+0000"
    try:
        return datetime.strptime(s, "%Y%m%d%H%M%S%z").astimezone(timezone.utc)
    except Exception:
        return None

def _strip_tags_preserve_text(s: str) -> str:
    # keep text & comment bodies; remove tags
    # first, extract comments if any
    comments = re.findall(r"<!--(.*?)-->", s, flags=re.S)
    ctext = " ".join([re.sub(r"\s+", " ", c).strip() for c in comments if c.strip()])
    # then strip tags
    no_tags = re.sub(r"<[^>]+>", "", s)
    no_tags = re.sub(r"\s+", " ", no_tags).strip()
    # prefer actual text; if none, use comment text
    return no_tags or ctext

def element_localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower() if tag else tag

def first_child_by_names(elem: ET.Element, names: Tuple[str, ...]) -> Optional[ET.Element]:
    wanted = {n.lower() for n in names}
    for child in list(elem):
        if element_localname(child.tag) in wanted:
            return child
    return None

def text_from_node(elem: Optional[ET.Element]) -> str:
    """
    Try very hard to extract visible text from a node:
    1) itertext()
    2) any non-empty attribute values (except 'lang'/'id' etc.)
    3) comment bodies within the node
    4) serialized XML with tags stripped
    """
    if elem is None:
        return ""

    # 1) normal text
    try:
        txt = "".join(elem.itertext()).strip()
        if txt:
            return txt
    except Exception:
        pass

    # 2) attributes (common weird case)
    for k, v in elem.attrib.items():
        lk = element_localname(k)
        if lk in ("lang", "id", "class", "type"):
            continue
        if v and str(v).strip():
            return str(v).strip()

    # 3 & 4) comments / strip tags
    try:
        raw = ET.tostring(elem, encoding="unicode")
        val = _strip_tags_preserve_text(raw)
        if val:
            return val
    except Exception:
        pass

    return ""

def pick_programme_text(prog: ET.Element) -> Tuple[str, str]:
    """
    Extract (title, description) from <programme> children first,
    then fall back to attributes on <programme> itself.
    """
    title_el = first_child_by_names(prog, ("title", "sub-title"))
    desc_el  = first_child_by_names(prog, ("desc",))

    title = text_from_node(title_el)
    desc  = text_from_node(desc_el)

    if not title:
        # fallback: attributes on <programme>, e.g. title="..."
        for k in ("title", "name", "sub-title", "subtitle"):
            v = prog.attrib.get(k)
            if v and v.strip():
                title = v.strip(); break
        if not title:
            # any attribute value except time/channel/lang-ish
            for k, v in prog.attrib.items():
                lk = element_localname(k)
                if lk in ("channel", "start", "stop", "lang", "id"):
                    continue
                if v and v.strip():
                    title = v.strip()
                    break

    if not desc:
        for k in ("desc", "description", "summary"):
            v = prog.attrib.get(k)
            if v and v.strip():
                desc = v.strip(); break

    return title.strip(), desc.strip()

# ----------------------- Supabase ----------------------

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
    for batch in chunked(rows, base_batch):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch)
                break
            except APIError as e:
                if attempt == MAX_RETRIES:
                    log.error("Giving up on %s batch (%d): %s", table, len(batch), e)
                else:
                    time.sleep(0.6*attempt)
                    log.warning("Retry %d/%d for %s (%d rows): %s", attempt, MAX_RETRIES, table, len(batch), e)
            except Exception as e:
                if attempt == MAX_RETRIES:
                    log.exception("Unexpected error upserting %s (%d rows): %s", table, len(batch), e)
                else:
                    time.sleep(0.6*attempt)
                    log.warning("Retry %d/%d for %s (%d rows) (unexpected): %s", attempt, MAX_RETRIES, table, len(batch), e)
    log.info("Upserted %d rows into %s.", total, table)

def refresh_mv(sb: Client):
    try:
        log.info("Refreshing MV via RPC: %s …", REFRESH_FUNC)
        sb.rpc(REFRESH_FUNC).execute()
        log.info("✅ MV refreshed.")
    except Exception as e:
        log.warning("MV refresh failed: %s", e)

# ----------------------- Fetch & Parse -----------------

def fetch_stream(url: str) -> io.BufferedReader:
    log.info("Fetching EPG (XML only): %s", url)
    resp = requests.get(
        url, stream=True, timeout=REQUEST_TIMEOUT,
        headers={
            "User-Agent": "Mozilla/5.0 (EPG Ingest; +https://github.com/)",
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.open-epg.com/",
        }
    )
    resp.raise_for_status()
    resp.raw.decode_content = True
    return resp.raw

def parse_open_epg_urls(urls: List[str]):
    now_utc = datetime.now(timezone.utc)
    horizon = now_utc + timedelta(hours=WINDOW_HOURS) if WINDOW_HOURS > 0 else None

    channels: Dict[str, dict] = {}
    programmes: Dict[str, dict] = {}

    empty_samples = []
    filled_samples = []
    raw_empty_snips = []
    titled_count = 0
    total_prog = 0

    for url in urls:
        try:
            stream = fetch_stream(url)
            context = ET.iterparse(stream, events=("start", "end"))
            _, root = next(context)

            for ev, el in context:
                if ev != "end":
                    continue
                tag = element_localname(el.tag)

                if tag == "channel":
                    ch_id = el.get("id")
                    if ch_id and ch_id not in channels:
                        disp_el = first_child_by_names(el, ("display-name",))
                        disp = text_from_node(disp_el) if disp_el is not None else ch_id
                        icon_el = first_child_by_names(el, ("icon",))
                        icon_url = None
                        if icon_el is not None:
                            for k, v in icon_el.attrib.items():
                                if element_localname(k) == "src" and v:
                                    icon_url = v.strip(); break
                        channels[ch_id] = {"id": ch_id, "display_name": disp or ch_id, "icon_url": icon_url}
                    el.clear()
                    continue

                if tag != "programme":
                    el.clear()
                    continue

                total_prog += 1

                ch_id = el.get("channel") or ""
                s = parse_xmltv_datetime(el.get("start"))
                e = parse_xmltv_datetime(el.get("stop"))
                if not (ch_id and s and e):
                    el.clear(); continue

                if horizon is not None:
                    if not (s <= horizon and e >= now_utc):
                        el.clear(); continue

                title, desc = pick_programme_text(el)

                if title or desc:
                    if len(filled_samples) < FILLED_SAMPLES_TO_LOG:
                        filled_samples.append((ch_id, s.isoformat(), title, len(desc)))
                    titled_count += 1
                else:
                    if len(empty_samples) < EMPTY_SAMPLES_TO_LOG:
                        empty_samples.append((ch_id, s.isoformat()))
                    if len(raw_empty_snips) < RAW_EMPTY_SNIPPETS:
                        try:
                            raw = ET.tostring(el, encoding="unicode")
                            # truncate for log readability
                            raw_empty_snips.append(raw[:600])
                        except Exception:
                            pass

                if ch_id not in channels:
                    channels[ch_id] = {"id": ch_id, "display_name": ch_id, "icon_url": None}

                if SKIP_EMPTY_TITLES and not title:
                    el.clear(); continue

                pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                programmes[pid] = {
                    "id": pid,
                    "channel_id": ch_id,
                    "start_time": s.isoformat(),
                    "end_time": e.isoformat(),
                    "title": (title or None),
                    "description": (desc or None),
                }

                el.clear()
                if (total_prog % 12000) == 0:
                    root.clear()

            ratio = (titled_count / total_prog) if total_prog else 0.0
            log.info("Parsed %s: channels(seen)=%d, programs_found=%d, kept=%d, titled_ratio=%.3f",
                     url, len(channels), total_prog, len(programmes), ratio)

        except Exception as e:
            log.exception("Error parsing %s: %s", url, e)

    if filled_samples:
        log.info("SAMPLE non-empty programmes (%d):", len(filled_samples))
        for ch, st, ti, dl in filled_samples:
            log.info("  ch=%s start=%s title=%r desc_len=%d", ch, st, ti, dl)
    if empty_samples:
        log.info("SAMPLE EMPTY-looking programmes (%d):", len(empty_samples))
        for ch, st in empty_samples:
            log.info("  ch=%s start=%s", ch, st)
    if raw_empty_snips:
        log.info("RAW empty-looking <programme> snippets (%d):", len(raw_empty_snips))
        for i, snip in enumerate(raw_empty_snips, 1):
            oneline = re.sub(r"\s+", " ", snip).strip()
            log.info("  #%d %s", i, oneline)

    return channels, programmes

# ----------------------- Main -------------------------

def main() -> int:
    log.info("Open-EPG ingest (PR, XML only). WINDOW_HOURS=%d, ENFORCE_LIVE=False, SKIP_EMPTY_TITLES=%s",
             WINDOW_HOURS, SKIP_EMPTY_TITLES)

    sb = init_supabase()

    if WINDOW_HOURS > 0:
        now_utc = datetime.now(timezone.utc)
        log.info("Windowing: ON  (%s -> %s UTC)", now_utc.isoformat(), (now_utc + timedelta(hours=WINDOW_HOURS)).isoformat())
    else:
        log.info("Windowing: OFF")

    channels, programmes = parse_open_epg_urls(EPG_URLS)

    # Optional targeted lookup
    if TEST_LOOKUP_CHANNEL and TEST_LOOKUP_START:
        sdt = parse_xmltv_datetime(TEST_LOOKUP_START)
        if sdt:
            found = False
            for row in programmes.values():
                if row["channel_id"] == TEST_LOOKUP_CHANNEL and row["start_time"] == sdt.isoformat():
                    log.info("TEST LOOKUP: %s @ %s -> title=%r, desc_len=%d",
                             TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START, row["title"], len(row.get("description") or ""))
                    found = True; break
            if not found:
                log.info("TEST LOOKUP: %s @ %s -> NOT FOUND in parsed set", TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START)
        else:
            log.info("TEST LOOKUP: could not parse TEST_LOOKUP_START=%r", TEST_LOOKUP_START)

    if channels:
        ch_rows = list(channels.values())
        log.info("Upserting %d channels …", len(ch_rows))
        upsert_with_retry(sb, "channels", ch_rows, conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels parsed.")

    prog_rows = list(programmes.values())
    log.info("Programs to upsert (deduped): %d", len(prog_rows))
    prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
    if prog_rows:
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programmes parsed (all filtered or parsing failed).")

    # DB sanity
    try:
        res_total = sb.table("programs").select("id", count="exact").execute()
        db_total = getattr(res_total, "count", 0) or 0
        res_titled = sb.table("programs").select("id", count="exact").not_.is_("title", "null").execute()
        db_titled = getattr(res_titled, "count", 0) or 0
        res_desc = sb.table("programs").select("id", count="exact").not_.is_("description", "null").execute()
        db_desced = getattr(res_desc, "count", 0) or 0
        log.info("DB sanity: total=%d, with_title=%d, with_desc=%d", db_total, db_titled, db_desced)
    except Exception as e:
        log.warning("DB sanity check failed: %s", e)

    refresh_mv(sb)
    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))
    return 0

if __name__ == "__main__":
    sys.exit(main())
