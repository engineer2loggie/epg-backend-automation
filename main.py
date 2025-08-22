#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, io, gzip, time, logging, itertools, random
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Dict, Optional

import requests
import xml.etree.ElementTree as ET
from supabase import create_client, Client
from postgrest.exceptions import APIError

# ----------------------- Config -----------------------

DEFAULT_EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
]

REQUEST_TIMEOUT = (10, 180)  # (connect, read)
BATCH_CHANNELS = 2000
BATCH_PROGRAMS = 1000        # smaller to avoid PostgREST payload issues
MAX_RETRIES = 4

# Refresh MV after ingest? (default yes)
REFRESH_MV = os.environ.get("REFRESH_MV", "1") not in ("0", "false", "False", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("epg")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

_raw_urls = os.environ.get("EPG_URLS", "")
EPG_URLS = [u.strip() for u in _raw_urls.split(",") if u.strip()] if _raw_urls else list(DEFAULT_EPG_URLS)


# ----------------------- Helpers ----------------------

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
    if not raw:
        return None
    s = raw.strip()

    # remove accidental space before tz
    if " " in s and (s.endswith("Z") or s[-5:-4] in ["+", "-"]):
        a, b = s.rsplit(" ", 1)
        s = a + b

    # normalize +HH:MM -> +HHMM
    if len(s) >= 6 and s[-3:] != "Z" and s[-3] == ":" and s[-6] in ["+", "-"]:
        s = s[:-3] + s[-2:]

    # add UTC if no tz
    if len(s) == 14:  # YYYYMMDDHHMMSS
        s += "+0000"

    # trailing Z -> +0000
    if s.endswith("Z") and len(s) == 15:
        s = s[:-1] + "+0000"

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

def preferred_text(elem: ET.Element, tag: str) -> Optional[str]:
    for child in elem.findall(tag):
        if child.text and child.text.strip():
            return child.text.strip()
    return None


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
    """Upsert rows with retries; if we hit server/duplicate-within-batch errors,
    split the batch (binary split) and retry smaller pieces."""
    total = 0
    queue: List[List[dict]] = list(chunked(rows, base_batch))

    while queue:
        batch = queue.pop(0)
        # de-dupe IDs inside the batch to prevent 21000
        if conflict == "id":
            dedup = {}
            for r in batch:
                k = r.get("id")
                if k is None:
                    continue
                keep = dedup.get(k)
                if keep is None:
                    dedup[k] = r
                else:
                    kd = (r.get("description") or "")
                    kd0 = (keep.get("description") or "")
                    kt = (r.get("title") or "")
                    kt0 = (keep.get("title") or "")
                    replace = False
                    if kt0.strip() == "No Title" and kt.strip() != "No Title":
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
                need_split = (
                    "21000" in msg or
                    "duplicate key value violates" in msg or
                    "500" in msg or
                    "413" in msg or
                    "Payload" in msg
                )
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

def refresh_next_24h_mv(sb: Client) -> None:
    """Calls the secure RPC to refresh the materialized view."""
    if not REFRESH_MV:
        log.info("Skipping materialized view refresh (REFRESH_MV disabled).")
        return
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info("Refreshing materialized view: programs_next_24h …")
            sb.rpc("refresh_programs_next_24h").execute()
            log.info("✅ Materialized view refreshed.")
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                log.error("❌ Failed to refresh MV after %d attempts: %s", attempt, e)
                return
            sleep_s = attempt * rand_jitter()
            log.warning("Retry %d/%d refreshing MV in %.2fs: %s", attempt, MAX_RETRIES, sleep_s, e)
            time.sleep(sleep_s)


# ----------------------- Core ingest ------------------

def fetch_and_process_epg(sb: Client, urls: List[str]):
    now_utc = datetime.now(timezone.utc)
    horizon_utc = now_utc + timedelta(hours=24)
    log.info("Window: %s -> %s (UTC)", now_utc.isoformat(), horizon_utc.isoformat())

    channels: Dict[str, dict] = {}  # id -> row
    programs: Dict[str, dict] = {}  # id -> row

    for url in urls:
        log.info("Fetching EPG: %s", url)
        try:
            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                stream = open_xml_stream(resp, url)
                context = ET.iterparse(stream, events=("start", "end"))
                _, root = next(context)

                c_new = 0
                p_seen = 0
                p_kept = 0

                for ev, el in context:
                    if ev != "end":
                        continue

                    tag = el.tag

                    if tag == "channel":
                        ch_id = el.get("id")
                        if ch_id:
                            name = preferred_text(el, "display-name") or ch_id
                            icon = el.find("icon").get("src") if el.find("icon") is not None else None
                            if ch_id not in channels:
                                channels[ch_id] = {"id": ch_id, "display_name": name, "icon_url": icon}
                                c_new += 1
                        el.clear()
                        continue

                    if tag == "programme":
                        p_seen += 1
                        ch_id = el.get("channel")
                        s = parse_xmltv_datetime(el.get("start"))
                        e = parse_xmltv_datetime(el.get("stop"))
                        if not (ch_id and s and e):
                            el.clear(); continue

                        # keep only items that overlap [now, now+24h]
                        if not (s <= horizon_utc and e >= now_utc):
                            el.clear(); continue

                        title = preferred_text(el, "title") or "No Title"
                        desc = preferred_text(el, "desc")

                        pid = f"{ch_id}_{s.strftime('%Y%m%d%H%M%S')}_{e.strftime('%Y%m%d%H%M%S')}"
                        row = {
                            "id": pid,
                            "channel_id": ch_id,
                            "start_time": s.isoformat(),
                            "end_time": e.isoformat(),
                            "title": title,
                            "description": desc
                        }

                        prev = programs.get(pid)
                        if prev is None:
                            programs[pid] = row
                            p_kept += 1
                        else:
                            prev_t = (prev.get("title") or "")
                            cand_t = (row.get("title") or "")
                            prev_d = (prev.get("description") or "") or ""
                            cand_d = (row.get("description") or "") or ""
                            replace = False
                            if prev_t.strip() == "No Title" and cand_t.strip() != "No Title":
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

                log.info("Parsed file done: channels=%d (new), programs_found=%d, programs_kept_24h=%d",
                         c_new, p_seen, p_kept)

        except requests.exceptions.RequestException as e:
            log.error("HTTP error for %s: %s", url, e)
        except ET.ParseError as e:
            log.error("XML parse error for %s: %s", url, e)
        except Exception as e:
            log.exception("Unexpected error for %s: %s", url, e)

    # Ensure all program.channel_id exist in channels
    referenced = {p["channel_id"] for p in programs.values()}
    missing = referenced.difference(channels.keys())
    if missing:
        log.warning("Creating %d placeholder channels for missing IDs.", len(missing))
        for ch in missing:
            channels[ch] = {"id": ch, "display_name": ch, "icon_url": None}

    # ---- Upserts ----
    if channels:
        upsert_with_retry(sb, "channels", list(channels.values()), conflict="id", base_batch=BATCH_CHANNELS)
    else:
        log.warning("No channels to upsert.")

    prog_rows = list(programs.values())
    log.info("Programs to upsert (deduped): %d", len(prog_rows))

    for sample in prog_rows[:3]:
        log.info("Sample program row: %s", {k: sample[k] for k in ("id", "channel_id", "start_time", "end_time", "title")})

    if prog_rows:
        prog_rows.sort(key=lambda r: (r["channel_id"], r["start_time"]))
        upsert_with_retry(sb, "programs", prog_rows, conflict="id", base_batch=BATCH_PROGRAMS)
    else:
        log.warning("No programs kept in 24h window. (Check time parsing/window)")

    # verify
    cnt = count_programs_in_window(sb, now_utc, horizon_utc)
    if cnt >= 0:
        log.info("✅ Supabase now has %d programs in the 24h window.", cnt)
    else:
        log.info("⚠️ Skipped verification count due to error.")

    # cleanup: old programs
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        sb.table("programs").delete().lt("end_time", cutoff.isoformat()).execute()
        log.info("Cleaned up programs with end_time < %s", cutoff.isoformat())
    except Exception as e:
        log.warning("Cleanup failed: %s", e)

    # refresh MV (secure RPC)
    refresh_next_24h_mv(sb)

    log.info("Done. Channels upserted: %d; Programs considered: %d", len(channels), len(prog_rows))


# ----------------------- Entrypoint -------------------

def main() -> int:
    log.info("EPG ingest starting. URLs: %s", ", ".join(EPG_URLS))
    sb = init_supabase()
    t0 = time.time()
    fetch_and_process_epg(sb, EPG_URLS)
    log.info("Finished in %.1fs", time.time() - t0)
    return 0

if __name__ == "__main__":
    sys.exit(main())
