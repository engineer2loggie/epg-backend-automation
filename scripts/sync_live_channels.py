#!/usr/bin/env python3
import os, sys, re, json, ssl
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from collections import Counter
from typing import Dict, Any, List, Tuple

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
COUNTRIES    = [c.strip().upper() for c in os.environ.get("COUNTRIES", "PR,DE,US,ES,MX,IT,IE,CA,AU,UK").split(",") if c.strip()]
SCRAPER_STRICT = os.environ.get("SCRAPER_STRICT", "1").strip() in ("1","true","TRUE","yes","YES")

SEARCH_BASE   = "https://iptv-org.github.io/?q=live%20country:{cc}"
CHANNELS_JSON = "https://iptv-org.github.io/api/channels.json"
STREAMS_JSON  = "https://iptv-org.github.io/api/streams.json"  # only for URL→channel id mapping
M3U_LINK_RE   = re.compile(r'href="([^"]*index\.m3u[^"]*)"', re.IGNORECASE)
ATTR_RE       = re.compile(r'([A-Za-z0-9_-]+)="([^"]*)"')

def require_env():
    miss = []
    if not SUPABASE_URL: miss.append("SUPABASE_URL")
    if not SERVICE_KEY:  miss.append("SUPABASE_SERVICE_ROLE_KEY")
    if miss: raise SystemExit(f"Missing env: {', '.join(miss)}")
    dom = SUPABASE_URL.split("//",1)[1].split("/",1)[0]
    print("ENV OK:", dom)
    print("COUNTRIES:", COUNTRIES)
    print("SCRAPER_STRICT:", SCRAPER_STRICT)

def fetch_text(url: str, timeout=90) -> str:
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent": "iptv-live-html/1.1"})
    with urlopen(req, context=ctx, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return resp.read().decode("utf-8", "ignore")

def fetch_json(url: str, timeout=90) -> Any:
    return json.loads(fetch_text(url, timeout=timeout))

def playlist_url_from_search(country: str) -> str:
    # Use Playwright to render the search page and extract the index.m3u link.
    from playwright.sync_api import sync_playwright
    url = SEARCH_BASE.format(cc=country)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (LiveScraper/1.1)")
        try:
            page.goto(url, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(1200)  # allow dynamic content to settle
            html = page.content()
        finally:
            browser.close()

    for href in M3U_LINK_RE.findall(html):
        if href.startswith("http"):
            return href
        # normalize relative link
        return f"https://iptv-org.github.io{href}"
    raise RuntimeError(f"No index.m3u link found on search page {url}")

def supabase_delete_country(cc: str):
    import urllib.request
    url = f"{SUPABASE_URL}/rest/v1/live_channels?country=eq.{cc}"
    req = urllib.request.Request(
        url=url, method="DELETE",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Prefer": "return=minimal"
        }
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        if resp.status not in (200,204):
            raise RuntimeError(f"DELETE {cc} failed: {resp.status}")

def upsert_rows(rows: List[Dict[str, Any]]):
    import urllib.request
    url = f"{SUPABASE_URL}/rest/v1/live_channels?on_conflict=channel_id"
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(
        url=url, data=data, method="POST",
        headers={
            "Content-Type": "application/json",
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Prefer": "resolution=merge-duplicates"
        },
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        if resp.status not in (200,201,204):
            body = resp.read().decode("utf-8", "ignore")
            raise RuntimeError(f"Supabase upsert failed {resp.status}: {body}")

def parse_m3u(text: str) -> List[Dict[str, str]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXTINF"):
            try:
                header, display = line.split(",", 1)
            except ValueError:
                header, display = line, ""
            attrs = dict(ATTR_RE.findall(header))
            j = i + 1
            while j < len(lines) and lines[j].startswith("#"):
                j += 1
            url = lines[j] if j < len(lines) else None
            if url:
                out.append({
                    "tvg_id"  : (attrs.get("tvg-id") or "").strip().lower(),
                    "tvg_name": (attrs.get("tvg-name") or "").strip(),
                    "tvg_logo": (attrs.get("tvg-logo") or "").strip(),
                    "display" : display.strip(),
                    "url"     : url.strip(),
                })
            i = j + 1
        else:
            i += 1
    return out

def norm_name(s: str) -> str:
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    s = re.sub(r'\b(hd|fullhd|uhd|4k|sd)\b', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def main():
    require_env()

    # Canonical channel catalog
    catalog = fetch_json(CHANNELS_JSON)
    by_id = { (c.get("id") or "").lower(): c for c in catalog if c.get("id") }

    # URL→channel ID map (for entries missing tvg-id)
    streams = fetch_json(STREAMS_JSON)
    url_to_id = {}
    for s in streams:
        cid = (s.get("channel") or "").lower()
        url = (s.get("url") or "").strip()
        if cid and url:
            url_to_id[url] = cid

    final_rows: List[Dict[str, Any]] = []
    kept_by_cc = Counter()

    for cc in COUNTRIES:
        print(f"[{cc}] Deleting existing rows in Supabase …")
        supabase_delete_country(cc)

        # Get the exact playlist the search page exposes
        print(f"[{cc}] Scraping search page …")
        try:
            plist_url = playlist_url_from_search(cc)
            print(f"[{cc}] Playlist via HTML: {plist_url}")
        except Exception as e:
            print(f"[{cc}] ERROR: {e}")
            if SCRAPER_STRICT:
                raise
            # Fallback if strict disabled (not recommended)
            plist_url = f"https://iptv-org.github.io/iptv/index.m3u?country={cc}&status=online"
            print(f"[{cc}] Fallback playlist: {plist_url}")

        m3u_text = fetch_text(plist_url)
        entries = parse_m3u(m3u_text)
        print(f"[{cc}] Parsed entries: {len(entries)}")

        rows_by_id: Dict[str, Dict[str, Any]] = {}
        now_iso = datetime.now(timezone.utc).isoformat()

        for e in entries:
            tvg_id   = e["tvg_id"]
            tvg_name = e["tvg_name"]
            tvg_logo = e["tvg_logo"]
            url      = e["url"]

            # Map to catalog id
            channel_id = None
            if tvg_id and tvg_id in by_id:
                channel_id = tvg_id
            elif url in url_to_id and url_to_id[url] in by_id:
                channel_id = url_to_id[url]
            else:
                # name match within country as a last resort
                # (kept minimal to avoid junk)
                pass

            if not channel_id:
                continue

            meta = by_id[channel_id]
            meta_cc = (meta.get("country") or "").upper()
            if meta_cc != cc:
                # enforce country from catalog
                continue

            if channel_id not in rows_by_id:
                rows_by_id[channel_id] = {
                    "channel_id":   channel_id,
                    "display_name": meta.get("name") or tvg_name or channel_id,
                    "country":      meta_cc,
                    "icon_url":     meta.get("logo") or (tvg_logo or None),
                    "stream_url":   url,
                    "check_time":   now_iso,
                    "source":       "iptv-org-html",
                    "updated_at":   now_iso,
                }

        rows = list(rows_by_id.values())
        print(f"[{cc}] Kept (deduped): {len(rows)}")
        kept_by_cc[cc] = len(rows)

        if rows:
            upsert_rows(rows)
            final_rows.extend(rows)

    print("Summary kept_by_country:", dict(kept_by_cc))
    print("Total upserted:", len(final_rows))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (URLError, HTTPError) as e:
        print(f"Network error: {e}", file=sys.stderr); sys.exit(2)
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr); sys.exit(1)
