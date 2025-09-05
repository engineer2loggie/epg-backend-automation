from __future__ import annotations

import os
import asyncio
import csv
from typing import List, Dict
from urllib.parse import urlparse
from datetime import datetime, timedelta

import pytz
from supabase import create_client, Client

# --- PARSER INTEGRATION ---
# IMPORTANT: Import all your parser classes and the Programme object here.
# The script assumes your parsers live in a `scripts/parsers/` directory.
# You will need to create/update a `scripts/parsers/__init__.py` file
# to make these imports work seamlessly.
from .parsers.gatotv import GatoTVParser # Assumed existing parser
from .parsers.tvguia_parser import TvGuiaParser, Programme # Your new parser and its Programme class

# This list powers the whole script. Add any new parser classes here.
ALL_PARSERS = [
    GatoTVParser,
    TvGuiaParser,
]

# -------------------- Config --------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

INPUT_MODE = os.getenv("INPUT_MODE", "supabase").lower()
CSV_PATH = os.getenv("CSV_PATH", "manual_tv_input.csv")

# This is now a FALLBACK timezone if one isn't specified in your input source.
LOCAL_TZ = os.getenv("LOCAL_TZ", "America/Mexico_City")
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
        supabase.table("mx_epg_scrape").delete().in_("programme_source_link", chunk).gte("programme_start_time", low).lte("programme_start_time", high).execute()

# -------------------- Inputs --------------------
async def read_links_from_supabase(supabase: Client) -> List[Dict[str, str]]:
    """Reads links and their timezones. Expects a 'timezone' column."""
    res = supabase.table("manual_tv_input").select("programme_source_link, timezone").execute()
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
    """Reads links and timezones from CSV. Expects a 'timezone' column."""
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
    """Selects the correct parser class for a given URL."""
    for p_class in ALL_PARSERS:
        if p_class.matches(url):
            return p_class() # Return an instance of the parser
    return None

async def scrape_one(source_info: Dict[str, str], *, hours_ahead: int) -> List[Programme]:
    """Scrapes a single source using its specific timezone."""
    url = source_info["url"]
    tzname = source_info["tz"]
    
    parser = pick_parser(url)
    if not parser:
        print(f"[warn] No parser found for {url}")
        return []
    try:
        progs = await parser.fetch_and_parse(url, tzname=tzname, hours_ahead=hours_ahead, page=None)
        return progs or []
    except Exception as e:
        print(f"[error] Failed to parse {url}: {e}")
        return []

async def scrape_all(sources: List[Dict[str, str]]) -> Dict[str, List[Programme]]:
    """Runs scraping concurrently for all provided sources."""
    results: Dict[str, List[Programme]] = {}
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    async def task(source: Dict[str, str]):
        async with sem:
            progs = await scrape_one(source, hours_ahead=HOURS_AHEAD)
            results[source["url"]] = progs

    await asyncio.gather(*(task(s) for s in sources))
    return results

def to_rows(programs_by_url: Dict[str, List[Programme]]) -> List[Dict]:
    """Converts Programme objects to dictionaries for Supabase, including new fields."""
    rows = []
    for url, progs in programs_by_url.items():
        for p in progs:
            rows.append({
                "programme_source_link": url,
                "programme_start_time": p.start.isoformat(),
                "programme_end_time": p.end.isoformat() if p.end else None,
                "programme_title": p.title,
                # Ensure your Supabase table has these columns
                "programme_desc": p.description,
                "programme_category": p.category,
            })
    return rows

def upsert_rows(supabase: Client, rows: List[Dict]):
    """Upserts rows to Supabase in chunks."""
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

    supported_hosts = tuple([d for parser in ALL_PARSERS for d in parser.domains])
    supported_sources = [s for s in sources if any(urlparse(s['url']).netloc.lower().endswith(h) for h in supported_hosts)]

    print(f"[info] Parsers active: {[p.__name__ for p in ALL_PARSERS]}")
    print(f"[info] Found {len(supported_sources)} supported sources to scrape.")

    # 1) Scrape all sources to get fresh data
    programs_by_url = await scrape_all(supported_sources)

    # Log results
    empty_sources = []
    for url, progs in programs_by_url.items():
        if progs:
            print(f"[parsed] OK ✓ {len(progs):>3} programmes from {url}")
        else:
            print(f"[parsed] FAIL ✗ 0 programmes from {url}")
            empty_sources.append(url)

    # 2) Purge the database ONLY for sources that returned new data
    if not DRY_RUN_PURGE:
        non_empty_sources = [u for u, p in programs_by_url.items() if p]
        if non_empty_sources:
            purge_window_for_sources(supabase, non_empty_sources, PURGE_HOURS_BACK, HOURS_AHEAD)
        else:
            print("[warn] Purge skipped: no sources produced rows.")
    else:
        print("[info] Dry run: purge step skipped.")

    # 3) Upsert the new rows
    rows = to_rows(programs_by_url)
    upsert_rows(supabase, rows)

    print(f"\n[done] Scrape complete. Total rows processed: {len(rows)}")
    if empty_sources:
        print("[note] The following sources returned 0 rows and were not purged:")
        for url in empty_sources:
            print(f"  - {url}")

if __name__ == "__main__":
    asyncio.run(main())
