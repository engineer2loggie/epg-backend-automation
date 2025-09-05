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
# This setup assumes your parsers live in a `scripts/parsers/` directory.
from .parsers.gatotv import GatoTVParser # Assumed existing parser
from .parsers.ontvtonight import OnTVTonightParser # Restoring your existing parser
from .parsers.tvguia_parser import TvGuiaParser, Programme # Your new parser and its Programme class

# This list powers the script. Add any new parser classes here.
ALL_PARSERS = [
    GatoTVParser,
    OnTVTonightParser,
    TvGuiaParser,
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
        supabase.table("mx_epg_scrape").delete().in_("programme_source_link", chunk).gte("programme_start_time", low).lte("programme_start_time", high).execute()

# -------------------- Inputs --------------------
async def read_links_from_supabase(supabase: Client) -> List[Dict[str, str]]:
    """
    Reads links from Supabase. It fetches all columns to avoid errors if 'timezone'
    is missing, but will use it if it exists.
    """
    # FIX: Select all columns (*) to prevent an error if 'timezone' does not exist.
    res = supabase.table("manual_tv_input").select("*").execute()
    links = [
        # The .get() method safely handles a missing 'timezone' key.
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
    """
    Selects the correct parser for a given URL by instantiating and checking each one.
    """
    # FIX: Instantiate the parser class before calling .matches() to prevent TypeError.
    for p_class in ALL_PARSERS:
        parser_instance = p_class()
        if parser_instance.matches(url):
            return parser_instance # Return the instance
    return None

async def scrape_one(source_info: Dict[str, str], *, hours_ahead: int) -> List[Programme]:
    """Scrapes a single source using its specific timezone."""
    url, tzname = source_info["url"], source_info["tz"]
    # FIX: pick_parser now returns an instance directly.
    parser = pick_parser(url)
    if not parser:
        print(f"[warn] No parser found for {url}")
        return []
    try:
        # 'parser' is already an instance, so we call the method on it.
        return await parser.fetch_and_parse(url, tzname=tzname, hours_ahead=hours_ahead, page=None) or []
    except Exception as e:
        print(f"[error] Failed to parse {url}: {e}")
        return []

async def scrape_all(sources: List[Dict[str, str]]) -> Dict[str, List[Programme]]:
    """Runs scraping concurrently for all provided sources."""
    results: Dict[str, List[Programme]] = {}
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    async def task(source: Dict[str, str]):
        async with sem:
            results[source["url"]] = await scrape_one(source, hours_ahead=HOURS_AHEAD)
    await asyncio.gather(*(task(s) for s in sources))
    return results

def to_rows(programs_by_url: Dict[str, List[Programme]]) -> List[Dict]:
    """
    SCHEMA-COMPLIANT: Converts Programme objects to dictionaries for Supabase.
    It combines title, category, and description into the single programme_title field.
    """
    rows = []
    for url, progs in programs_by_url.items():
        for p in progs:
            # --- Start of Schema-Specific Logic ---
            # Build the combined title string
            # 1. Start with the base title
            full_title = p.title if p.title else "No Title"

            # 2. Add category if it exists
            if p.category:
                full_title += f" [{p.category}]"

            # 3. Add description if it exists
            if p.description:
                full_title += f" - {p.description}"
            # --- End of Schema-Specific Logic ---

            # Your schema requires a non-null end time. If a parser fails to find one,
            # we'll add a default 30-minute duration as a safe fallback.
            end_time = p.end if p.end else p.start + timedelta(minutes=30)

            rows.append({
                "programme_source_link": url,
                "programme_start_time": p.start.isoformat(),
                "programme_end_time": end_time.isoformat(),
                "programme_title": full_title,
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

    # 1) Scrape all sources
    programs_by_url = await scrape_all(supported_sources)

    # Log results
    empty_sources = []
    for url, progs in programs_by_url.items():
        if progs:
            print(f"[parsed] OK ✓ {len(progs):>3} programmes from {url}")
        else:
            print(f"[parsed] FAIL ✗ 0 programmes from {url}")
            empty_sources.append(url)

    # 2) Purge the database for sources that returned new data
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

    print(f"\n[done] Scrape complete. Total rows processed: {len(rows)}")
    if empty_sources:
        print("[note] The following sources returned 0 rows and were not purged:")
        for url in empty_sources:
            print(f"  - {url}")

if __name__ == "__main__":
    asyncio.run(main())

