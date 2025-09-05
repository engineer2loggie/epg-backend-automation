from __future__ import annotations

import os
import asyncio
import csv
from typing import List, Dict, Any
from urllib.parse import urlparse
from datetime import datetime, timedelta

import pytz
from supabase import create_client, Client

# --- PARSER INTEGRATION ---
# IMPORTANT: Import all your parser classes here.
# Existing parsers:
from .parsers.gatotv import GatoTVParser
from .parsers.ontvtonight import OnTVTonightParser

# NEW: Laocho parser replaces TvGuia.
from .parsers.laocho import LaochoParser  # <-- add this

# REMOVE: TvGuiaParser + Programme from tvguia
# from .parsers.tvguia_parser import TvGuiaParser, Programme

# This list powers the script. Add any new parser classes here.
ALL_PARSERS = [
    GatoTVParser,
    OnTVTonightParser,
    LaochoParser,          # <-- new parser in the rotation
]

# -------------------- Config --------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

INPUT_MODE = os.getenv("INPUT_MODE", "supabase").lower()
CSV_PATH = os.getenv("CSV_PATH", "manual_tv_input.csv")

# Fallback timezone if one isn't specified in your input source.
LOCAL_TZ = os.getenv("LOCAL_TZ", "America/Mexico_city")
HOURS_AHEAD = int(os.getenv("HOURS_AHEAD", "36"))
SCRAPE_CONCURRENCY = int(os.getenv("SCRAPE_CONCURRENCY", "4"))

PURGE_HOURS_BACK = int(os.getenv("PURGE_HOURS_BACK", "24"))
DRY_RUN_PURGE = os.getenv("DRY_RUN_PURGE", "0") == "1"

# -------------------- Utilities --------------------
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def _dedupe_rows_by_pk(rows: List[dict]) -> List[dict]:
    """Remove duplicates to avoid PostgREST errors."""
    seen, out = set(), []
    for r in rows:
        key = (r["programme_source_link"], r["programme_start_time"])
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out

def purge_window_for_sources(supabase: Client, sources: list[str], hours_back: int, hours_ahead: int):
    """Delete existing rows for given sources in the specified time window."""
    if not sources:
        return
    now = datetime.now(pytz.UTC)
    low = (now - timedelta(hours=hours_back)).isoformat()
    high = (now + timedelta(hours=hours_ahead)).isoformat()

    CHUNK = 100
    for i in range(0, len(sources), CHUNK):
        chunk = sources[i:i+CHUNK]
        print(f"Purging {len(chunk)} sources from {low} to {high}...")
        supabase.table("mx_epg_scrape").delete()\
            .in_("programme_source_link", chunk)\
            .gte("programme_start_time", low)\
            .lte("programme_start_time", high)\
            .execute()

# -------------------- Inputs --------------------
async def read_links_from_supabase(supabase: Client) -> List[Dict[str, str]]:
    res = supabase.table("manual_tv_input").select("*").execute()
    links = [
        {"url": row.get("programme_source_link"), "tz": row.get("timezone") or LOCAL_TZ}
        for row in res.data if row.get("programme_source_link")
    ]
    seen, out = set(), []
    for item in links:
        if item["url"] not in seen:
            seen.add(item["url"])
            out.append(item)
    return out

def read_links_from_csv(path: str) -> List[Dict[str, str]]:
    items = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        url_key = next((k for k in ("programme_source_link", "url") if k in reader.fieldnames), None)
        if not url_key:
            raise RuntimeError(f"CSV missing a URL column; fields={reader.fieldnames}")
        for r in reader:
            if r.get(url_key):
                items.append({"url": r.get(url_key).strip(), "tz": r.get("timezone", "").strip() or LOCAL_TZ})

    seen, out = set(), []
    for item in items:
        if item["url"] not in seen:
            seen.add(item["url"])
            out.append(item)
    return out

# -------------------- Orchestration --------------------
def pick_parser(url: str):
    """Selects the correct parser for a given URL by instantiating and checking each one."""
    for p_class in ALL_PARSERS:
        parser_instance = p_class()
        if parser_instance.matches(url):
            return parser_instance
    return None

async def scrape_one(source_info: Dict[str, str], *, hours_ahead: int) -> List[Any]:
    """Scrapes a single source using its specific timezone."""
    url, tzname = source_info["url"], source_info["tz"]
    parser = pick_parser(url)
    if not parser:
        print(f"[warn] No parser found for {url}")
        return []
    try:
        return await parser.fetch_and_parse(url, tzname=tzname, hours_ahead=hours_ahead, page=None) or []
    except Exception as e:
        print(f"[error] Failed to parse {url}: {e}")
        return []

async def scrape_all(sources: List[Dict[str, str]]) -> Dict[str, List[Any]]:
    results: Dict[str, List[Any]] = {}
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    async def task(source: Dict[str, str]):
        async with sem:
            results[source["url"]] = await scrape_one(source, hours_ahead=HOURS_AHEAD)
    await asyncio.gather(*(task(s) for s in sources))
    return results

def to_rows(programs_by_url: Dict[str, List[Any]]) -> List[Dict]:
    """
    SCHEMA-COMPLIANT: Converts Programme-like objects to dictionaries for Supabase.
    Combines title + optional category/description into programme_title.
    """
    rows = []
    for url, progs in programs_by_url.items():
        for p in progs:
            full_title = getattr(p, "title", None) or "No Title"
            category = getattr(p, "category", None)
            description = getattr(p, "description", None)
            if category:
                full_title += f" [{category}]"
            if description:
                full_title += f" - {description}"

            start = getattr(p, "start", None)
            end = getattr(p, "end", None)
            if start is None:
                # skip malformed items
                continue
            if end is None:
                end = start + timedelta(minutes=30)

            rows.append({
                "programme_source_link": url,
                "programme_start_time": start.isoformat(),
                "programme_end_time": end.isoformat(),
                "programme_title": full_title,
            })
    return rows

def upsert_rows(supabase: Client, rows: List[Dict]):
    rows = _dedupe_rows_by_pk(rows)
    if not rows:
        print("[info] Nothing to upsert.")
        return

    print(f"[info] Upserting {len(rows)} rows to Supabase...")
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        try:
            supabase.table("mx_epg_scrape").upsert(
                chunk, on_conflict="programme_source_link,programme_start_time"
            ).execute()
        except Exception as e:
            print(f"[error] Supabase upsert failed: {e}")

# -------------------- Main --------------------
async def main():
    supabase = get_supabase()

    if INPUT_MODE == "csv":
        sources = read_links_from_csv(CSV_PATH)
    else:
        sources = await read_links_from_supabase(supabase)

    # Only keep URLs supported by at least one parser (based on their .domains list)
    supported_hosts = tuple([d for parser in ALL_PARSERS for d in parser.domains])
    supported_sources = [s for s in sources if any(urlparse(s['url']).netloc.lower().endswith(h) for h in supported_hosts)]

    print(f"[info] Parsers active: {[p.__name__ for p in ALL_PARSERS]}")
    print(f"[info] Found {len(supported_sources)} supported sources to scrape.")

    # 1) Scrape all sources
    programs_by_url = await scrape_all(supported_sources)

    # 2) Purge (only for sources that yielded rows)
    if not DRY_RUN_PURGE:
        non_empty_sources = [u for u, p in programs_by_url.items() if p]
        if non_empty_sources:
            purge_window_for_sources(supabase, non_empty_sources, PURGE_HOURS_BACK, HOURS_AHEAD)
        else:
            print("[warn] Purge skipped: no sources produced rows.")
    else:
        print("[info] Dry run: purge step skipped.")

    # 3) Convert to rows and upsert
    rows = to_rows(programs_by_url)
    upsert_rows(supabase, rows)

    # 4) Log summary
    empty_sources = [u for u, p in programs_by_url.items() if not p]
    for url, progs in programs_by_url.items():
        print(f"[parsed] {'OK ✓' if progs else 'FAIL ✗'} {len(progs):>3} from {url}")

    print(f"\n[done] Scrape complete. Total rows processed: {len(rows)}")
    if empty_sources:
        print("[note] The following sources returned 0 rows and were not purged:")
        for url in empty_sources:
            print(f"  - {url}")

if __name__ == "__main__":
    asyncio.run(main())
