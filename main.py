#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import io
import re
import time
import gzip
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Iterable, Tuple

import requests
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter, Retry
from supabase import create_client, Client
from postgrest.exceptions import APIError

# =========================
# Logging
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("open-epg")

# =========================
# Env / Config
# =========================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()

# Comma-separated list of direct XML URLs (no .gz needed, but .gz ok too)
OPEN_EPG_FILES = [u.strip() for u in os.environ.get("OPEN_EPG_FILES", "").split(",") if u.strip()]
if not OPEN_EPG_FILES:
    # sensible default for PR pilot
    OPEN_EPG_FILES = [
        "https://www.open-epg.com/files/puertorico1.xml",
        "https://www.open-epg.com/files/puertorico2.xml",
    ]

# windowing off by default for debugging (0 => keep everything)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "0") or "0")

# Language preference order for title/desc selection
PREFER_LANGS = [p.strip().lower() for p in os.environ.get("PREFER_LANGS", "es-pr,es,en").split(",") if p.strip()]

# Skip rows that end up with empty titles?
SKIP_EMPTY_TITLES = os.environ.get("SKIP_EMPTY_TITLES", "0").lower() in ("1", "true", "yes")

# Debug: dump how many inner programme children to logs
DEBUG_DUMP_PROGRAM_CHILDREN = int(os.environ.get("DEBUG_DUMP_PROGRAM_CHILDREN", "10") or "10")

# Optional: test lookup for a single exact slot
TEST_LOOKUP_CHANNEL = os.environ.get("TEST_LOOKUP_CHANNEL", "").strip()
TEST_LOOKUP_START = os.environ.get("TEST_LOOKUP_START", "").strip()  # e.g. "20250822223000 +0000"

# Save the fetched XML to artifacts (if workflow uploads them)
SAVE_XML = os.environ.get("SAVE_XML", "0").lower() in ("1", "true", "yes")

# Refresh MV afterwards
REFRESH_MV = os.environ.get("REFRESH_MV", "1").lower() in ("1", "true", "yes")
REFRESH_MV_FUNC = os.environ.get("REFRESH_MV_FUNC", "refresh_programs_next_12h").strip()  # your DB has this

REQUEST_TIMEOUT = (15, 240)

# =========================
# HTTP session (spoof browser)
# =========================
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:116.0) "
        "Gecko/20100101 Firefox/116.0"
    ),
    "Accept": "text/xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": ",".join(PREFER_LANGS) if PREFER_LANGS else "en-US,en;q=0.8",
    "Referer": "https://www.open-epg.com/app/index.php",
    "Connection": "keep-alive",
}

def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    retries = Retry(
        total=5, backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def fetch_url_bytes(url: str) -> bytes:
    s = new_session()
    with s.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
        resp.raise_for_status()
        raw = resp.raw
        raw.decode_content = True  # honor Content-Encoding
        data = raw.read()
    # If the URL ends with .gz but server didn't set Content-Encoding, decompress here.
    if url.lower().endswith(".gz"):
        try:
            return gzip.decompress(data)
        except Exception:
            # maybe already decompressed by requests
            return data
    return data

def fetch_xml_text(url: str) -> str:
    b = fetch_url_bytes(url)
    # try to respect XML declaration encoding
    head = b[:200].decode("ascii", errors="ignore")
    m = re.search(r'encoding=["\']([A-Za-z0-9_\-]+)["\']', head)
    enc = m.group(1) if m else "utf-8"
    try:
        return b.decode(enc, errors="replace")
    except LookupError:
        return b.decode("utf-8", errors="replace")

def smoke_on_xml(xml_text: str, label: str) -> None:
    titles = re.findall(r"<title[^>]*>([^<]+)</title>", xml_text, flags=re.I)
    descs  = re.findall(r"<desc[^>]*>([^<]+)</desc>", xml_text, flags=re.I)
    log.info("SMOKE[%s]: non-empty <title>=%d, non-empty <desc>=%d", label, len(titles), len(descs))
    # peek some inner tags for known PR channel
    ch_pat = re.compile(
        r'<programme[^>]*channel="3ABN LATINO WTPM DT3 PUERTO RICO\.pr"[^>]*>(.*?)</programme>',
        flags=re.S | re.I)
    m = ch_pat.search(xml_text)
    if m:
        inner = m.group(1)
        bits = re.findall(r"<(title|sub-title|desc)[^>]*>.*?</\1>", inner, flags=re.S | re.I)
        sample = [t[:140] + ("…" if len(t) > 140 else "") for t in bits[:3]]
        log.info("SMOKE[%s]: 3ABN inner(3) => %s", label, sample)

# =========================
# XML helpers
# =========================
def localname(tag: str) -> str:
    if not tag: return tag
    return tag.split("}", 1)[1] if tag.startswith("{") else tag

def parse_xmltv_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    s = raw.strip()
    # remove accidental space before tz (e.g. '...  +0000')
    if " " in s:
        a, b = s.rsplit(" ", 1)
        s = a + b
    # normalize +HH:MM -> +HHMM
    if len(s) >= 6 and (not s.endswith("Z")) and s[-3] == ":" and s[-5] in "+-":
        s = s[:-3] + s[-2:]
    # trailing Z => +0000
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

def collect_lang_texts(elem: ET.Element, tag: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for child in list(elem):
        if localname(child.tag).lower() != tag:
            continue
        lang = (child.attrib.get("lang") or "").lower()
        txt = "".join(child.itertext()).strip()
        if txt:
            out.append((lang, txt))
    return out

def prefer_lang(pairs: List[Tuple[str, str]], prefer: List[str]) -> Optional[str]:
    # 1) exact lang match order
    langs = [l for l, _ in pairs]
    for want in prefer:
        for (l, t) in pairs:
            if l == want and t.strip():
                return t.strip()
    # 2) prefix match (es-pr ~ es)
    pref_bases = [p.split("-", 1)[0] for p in prefer if "-" in p]
    for want in pref_bases:
        for (l, t) in pairs:
            if l.split("-", 1)[0] == want and t.strip():
                return t.strip()
    # 3) any non-empty
    for (_, t) in pairs:
        if t.strip():
            return t.strip()
    return None

# =========================
# Supabase
# =========================
def init_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        log.error("❌ SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        raise SystemExit(1)
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("✅ Connected to Supabase.")
        return sb
    except Exception as e:
        log.exception("Failed to create Supabase client: %s", e)
        raise SystemExit(1)

def chunked(seq: Iterable[dict], size: int) -> Iterable[List[dict]]:
    block: List[dict] = []
    for item in seq:
        block.append(item)
        if len(block) >= size:
            yield block
            block = []
    if block:
        yield block

def upsert_with_retry(sb: Client, table: str, rows: List[dict], conflict: str, batch_size: int = 1000) -> None:
    total = 0
    for batch in chunked(rows, batch_size):
        for attempt in range(1, 5):
            try:
                sb.table(table).upsert(batch, on_conflict=conflict).execute()
                total += len(batch)
                break
            except APIError as e:
                msg = str(e)
                # split batch on server/size/dup errors
                if ("duplicate key value violates" in msg or "21000" in msg or "413" in msg or "Payload" in msg) and len(batch) > 1:
                    mid = len(batch) // 2
                    # recursive split
                    upsert_with_retry(sb, table, batch[:mid], conflict, batch_size)
                    upsert_with_retry(sb, table, batch[mid:], conflict, batch_size)
                    break
                if attempt == 4:
                    log.error("Giving up on %s batch (%d): %s", table, len(batch), msg)
                else:
                    time.sleep(0.5 * attempt)
            except Exception as e:
                if attempt == 4:
                    log.exception("Unexpected error upserting %s (%d): %s", table, len(batch), e)
                else:
                    time.sleep(0.5 * attempt)
    log.info("Upserted %d rows into %s.", total, table)

def refresh_materialized_view(sb: Client) -> None:
    if not REFRESH_MV:
        return
    func = REFRESH_MV_FUNC or "refresh_programs_next_12h"
    for attempt in range(1, 5):
        try:
            log.info("Refreshing MV via RPC: %s …", func)
            sb.rpc(func).execute()
            log.info("✅ MV refreshed.")
            return
        except Exception as e:
            if attempt == 4:
                log.warning("RPC %s failed after retries: %s", func, e)
                return
            log.warning("Retry %d/4 for %s: %s", attempt, func, e)
            time.sleep(0.5 * attempt)

# =========================
# Core parsing
# =========================
def parse_open_epg_xml_text(xml_text: str,
                            now_utc: Optional[datetime],
                            horizon_utc: Optional[datetime],
                            prefer_langs: List[str],
                            skip_empty_titles: bool,
                            debug_dump_limit: int = 10):
    seen_channels: Dict[str, dict] = {}
    programs: List[dict] = []

    # For memory safety use iterparse from bytes
    context = ET.iterparse(io.BytesIO(xml_text.encode("utf-8")), events=("start", "end"))
    _, root = next(context)

    dumped = 0
    titled = 0

    for ev, el in context:
        if ev != "end":
            continue
        tag = localname(el.tag).lower()

        if tag == "channel":
            ch_id = el.attrib.get("id") or ""
            if ch_id and ch_id not in seen_channels:
                disp = None
                # try first display-name
                for child in list(el):
                    if localname(child.tag).lower() == "display-name":
                        disp = "".join(child.itertext()).strip()
                        if disp:
                            break
                icon = None
                for child in list(el):
                    if localname(child.tag).lower() == "icon":
                        # <icon src="...">
                        for k, v in child.attrib.items():
                            if localname(k).lower() == "src" and v:
                                icon = v.strip()
                                break
                seen_channels[ch_id] = {
                    "id": ch_id,
                    "display_name": disp or ch_id,
                    "icon_url": icon
                }
            el.clear()
            continue

        if tag == "programme":
            ch_id = el.attrib.get("channel") or ""
            s = parse_xmltv_datetime(el.attrib.get("start"))
            e = parse_xmltv_datetime(el.attrib.get("stop"))
            if not (ch_id and s and e):
                el.clear()
                continue

            # windowing
            if now_utc and horizon_utc:
                if not (s <= horizon_utc and e >= now_utc):
                    el.clear(); continue

            # collect multilingual texts
            titles = collect_lang_texts(el, "title")
            subs   = collect_lang_texts(el, "sub-title")
            descs  = collect_lang_texts(el, "desc")

            chosen_title = prefer_lang(titles, prefer_langs) or prefer_lang(subs, prefer_langs) or ""
            chosen_desc  = prefer_lang(descs, prefer_langs) or ""

            if chosen_title.strip():
                titled += 1

            if debug_dump_limit > 0 and dumped < debug_dump_limit:
                # show children we saw
                lines: List[str] = []
                if titles:
                    lines.append(f"    <title candidates> {[(l or '', t[:40]) for (l,t) in titles]}")
                else:
                    lines.append("    <title> ∅")
                if subs:
                    lines.append(f"    <sub-title candidates> {[(l or '', t[:40]) for (l,t) in subs]}")
                if descs:
                    lines.append(f"    <desc candidates> {[(l or '', t[:60]) for (l,t) in descs]}")
                else:
                    lines.append("    <desc> ∅")
                lines.append(f"    -> chosen title='{chosen_title[:60]}', desc_len={len(chosen_desc)}")
                log.info("DEBUG children ch=%s start=%s\n%s",
                         ch_id, s.isoformat(), "\n".join(lines))
                dumped += 1

            if skip_empty_titles and not chosen_title.strip():
                el.clear()
                continue

            pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
            programs.append({
                "id": pid,
                "channel_id": ch_id,
                "start_time": s.isoformat(),
                "end_time": e.isoformat(),
                "title": chosen_title if chosen_title else None,
                "description": chosen_desc if chosen_desc else None,
            })
            el.clear()
            continue

        el.clear()
        if (len(programs) % 8000) == 0:
            root.clear()

    ratio = (titled / max(1, len(programs))) if programs else 0.0
    return seen_channels, programs, ratio

def test_lookup(programs: List[dict], ch_id: str, raw_start: str) -> None:
    if not (ch_id and raw_start):
        return
    s = parse_xmltv_datetime(raw_start)
    if not s:
        log.info("TEST LOOKUP: bad start format: %s", raw_start); return
    sid = s.strftime('%Y%m%d%H%M%S')
    for p in programs:
        if p["channel_id"] == ch_id and p["id"].startswith(f"{ch_id}_{sid}_"):
            log.info("TEST LOOKUP: %s @ %s -> title=%r, desc=%r",
                     ch_id, raw_start, p.get("title"), (p.get("description") or "")[:200])
            return
    log.info("TEST LOOKUP: %s @ %s -> NOT FOUND in parsed set.", ch_id, raw_start)

# =========================
# Main ingest
# =========================
def main() -> int:
    log.info("Open-EPG ingest (PR, XML only). WINDOW_HOURS=%d, ENFORCE_LIVE=False, SKIP_EMPTY_TITLES=%s, PREFER_LANGS=%s",
             WINDOW_HOURS, SKIP_EMPTY_TITLES, ",".join(PREFER_LANGS) if PREFER_LANGS else "(none)")

    sb = init_supabase()

    # Window bounds
    now_utc = None
    horizon_utc = None
    if WINDOW_HOURS and WINDOW_HOURS > 0:
        now_utc = datetime.now(timezone.utc)
        horizon_utc = now_utc + timedelta(hours=WINDOW_HOURS)
        log.info("Windowing: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())
    else:
        log.info("Windowing: OFF")

    channels: Dict[str, dict] = {}
    programs_all: List[dict] = []

    for url in OPEN_EPG_FILES:
        log.info("Fetching EPG (XML only): %s", url)
        xml_text = fetch_xml_text(url)

        if SAVE_XML:
            Path("/tmp").mkdir(parents=True, exist_ok=True)
            outp = f"/tmp/open_epg_fetch_{Path(url).name}"
            Path(outp).write_text(xml_text, encoding="utf-8")

        smoke_on_xml(xml_text, Path(url).name)

        chs, progs, ratio = parse_open_epg_xml_text(
            xml_text=xml_text,
            now_utc=now_utc,
            horizon_utc=horizon_utc,
            prefer_langs=PREFER_LANGS,
            skip_empty_titles=SKIP_EMPTY_TITLES,
            debug_dump_limit=DEBUG_DUMP_PROGRAM_CHILDREN
        )

        channels.update(chs)
        programs_all.extend(progs)
        log.info("Parsed %s: channels(seen)=%d, programs_found=%d, kept=%d, titled_ratio=%.3f",
                 url, len(chs), len(progs), len(progs), ratio)

    # Test a specific known slot if provided
    if TEST_LOOKUP_CHANNEL and TEST_LOOKUP_START:
        test_lookup(programs_all, TEST_LOOKUP_CHANNEL, TEST_LOOKUP_START)

    # Upserts
    if channels:
        upsert_with_retry(sb, "channels",
                          [{"id": k, "display_name": v["display_name"], "icon_url": v["icon_url"]} for k, v in channels.items()],
                          conflict="id", batch_size=2000)
    else:
        log.warning("No channels parsed!")

    log.info("Programs to upsert (deduped): %d", len(programs_all))
    if programs_all:
        # stable order
        programs_all.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_with_retry(sb, "programs", programs_all, conflict="id", batch_size=1000)
    else:
        log.warning("No programmes parsed (check content/headers/window).")

    # Optional: refresh MV
    refresh_materialized_view(sb)

    log.info("Finished. Channels: %d; Programs: %d", len(channels), len(programs_all))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
