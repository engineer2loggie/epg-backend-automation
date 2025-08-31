from __future__ import annotations

import os
import asyncio
import csv
from typing import List, Dict
from urllib.parse import urlparse
from datetime import datetime, timedelta

import pytz
from supabase import create_client, Client
from playwright.async_api import async_playwright

from .parsers import ALL_PARSERS
from .parsers.base import Programme

# -------------------- Config --------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

INPUT_MODE = os.getenv("INPUT_MODE", "supabase").lower()     # "supabase" or "csv"
CSV_PATH   = os.getenv("CSV_PATH", "manual_tv_input.csv")

LOCAL_TZ   = os.getenv("LOCAL_TZ", "America/Mexico_City")
HOURS_AHEAD = int(os.getenv("HOURS_AHEAD", "36"))
SCRAPE_CONCURRENCY = int(os.getenv("SCRAPE_CONCURRENCY", "4"))

# Purge window so corrected rows replace stale ones (e.g., after timezone/title fix)
PURGE_HOURS_BACK = int(os.getenv("PURGE_HOURS_BACK", "24"))

# -------------------- Utilities --------------------
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def _dedupe_keep_order(seq: List[str]) -> List[str]:
    seen, out = set(), []
    for s in seq:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def _dedupe_rows_by_pk(rows: List[dict]) -> List[dict]:
    """
    PK is (programme_source_link, programme_start_time).
    Remove duplicates within the batch to avoid PostgREST 21000 errors.
    """
    seen, out = set(), []
    for r in rows:
        key = (r["programme_source_link"], r["programme_start_time"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def purge_window_for_sources(supabase: Client, sources: list[str], hours_back: int, hours_ahead: int):
    """Delete existing rows for these sources in [now-hours_back, now+hours_ahead] so new scrape replaces them."""
    if not sources:
        return
    now = datetime.now(pytz.UTC)
    low = (now - timedelta(hours=hours_back)).isoformat()
    high = (now + timedelta(hours=hours_ahead)).isoformat()

    CHUNK = 100
    for i in range(0, len(sources), CHUNK):
        chunk = sources[i:i+CHUNK]
        supabase.table("mx_epg_scrape") \
            .delete() \
            .in_("programme_source_link", chunk) \
            .gte("programme_start_time", low) \
            .lte("programme_start_time", high) \
            .execute()

# -------------------- Inputs --------------------
async def read_links_from_supabase(supabase: Client) -> List[str]:
    """Reads public.manual_tv_input.programme_source_link (PK)."""
    res = supabase.table("manual_tv_input").select("programme_source_link").execute()
    links = [row.get("programme_source_link") for row in res.data if row.get("programme_source_link")]
    return _dedupe_keep_order(links)

def read_links_from_csv(path: str) -> List[str]:
    """CSV fallback accepting programme_source_link or legacy program_source_link."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        key = None
        for cand in ("programme_source_link", "program_source_link", "source_link", "url"):
            if cand in reader.fieldnames:
                key = cand
                break
        if not key:
            raise RuntimeError(f"CSV missing programme_source_link column; fields={reader.fieldnames}")
        rows = [r.get(key) for r in reader if r.get(key)]
    return _dedupe_keep_order(rows)

# -------------------- Orchestration --------------------
def pick_parser(url: str):
    for p in ALL_PARSERS:
        if p.matches(url):
            return p
    return None

async def scrape_one(url: str, context, *, tzname: str, hours_ahead: int) -> List[Programme]:
    """
    Let each parser decide whether to use the page (Playwright) or do httpx on its own.
    We hand it 'page' from the shared context when needed.
    """
    parser = pick_parser(url)
    if not parser:
        return []
    # Only open a page if the parser will actually use it (dom/JS sites).
    # Heuristic by domain: OnTVTonight & GatoTV are static (no page needed).
    host = urlparse(url).netloc.lower()
    needs_page = not (host.endswith("ontvtonight.com") or host.endswith("gatotv.com"))

    page = await context.new_page() if needs_page else None
    try:
        return await parser.fetch_and_parse(url, tzname=tzname, hours_ahead=hours_ahead, page=page)
    except Exception as e:
        print(f"[warn] failed to parse {url}: {e}")
        return []
    finally:
        if page:
            await page.close()

async def scrape_all(urls: List[str]) -> Dict[str, List[Programme]]:
    results: Dict[str, List[Programme]] = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(ignore_https_errors=True)

        sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)

        async def task(u: str):
            async with sem:
                progs = await scrape_one(u, context, tzname=LOCAL_TZ, hours_ahead=HOURS_AHEAD)
                if progs:
                    results[u] = progs

        await asyncio.gather(*(task(u) for u in urls))

        await context.close()
        await browser.close()
    return results

def to_rows(programs_by_url: Dict[str, List[Programme]]):
    rows = []
    for url, progs in programs_by_url.items():
        for p in progs:
            rows.append({
                "programme_source_link": url,
                "programme_start_time": p.start.isoformat(),
                "programme_end_time":   p.end.isoformat(),
                "programme_title":      p.title,
            })
    return rows

def upsert_rows(supabase: Client, rows):
    rows = _dedupe_rows_by_pk(rows)
    if not rows:
        print("[info] nothing to upsert")
        return
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i+CHUNK]
        res = supabase.table("mx_epg_scrape") \
            .upsert(chunk, on_conflict="programme_source_link,programme_start_time") \
            .execute()
        if getattr(res, "error", None):
            print("[error]", res.error)
        else:
            print(f"[upsert] {len(chunk)} rows")

# -------------------- Main --------------------
async def main():
    supabase = get_supabase()

    if INPUT_MODE == "csv":
        urls = read_links_from_csv(CSV_PATH)
    else:
        urls = await read_links_from_supabase(supabase)

    # Filter to domains actually supported by registered parsers
    supported_hosts = tuple([d for parser in ALL_PARSERS for d in parser.domains])
    urls = [u for u in urls if any(urlparse(u).netloc.lower().endswith(h) for h in supported_hosts)]
    urls = _dedupe_keep_order(urls)

    # Purge recent window for these sources so corrected rows replace bad ones
    purge_window_for_sources(supabase, urls, PURGE_HOURS_BACK, HOURS_AHEAD)

    programs_by_url = await scrape_all(urls)
    rows = to_rows(programs_by_url)
    upsert_rows(supabase, rows)

    print(f"[done] total rows: {len(rows)}")

if __name__ == "__main__":
    asyncio.run(main())
