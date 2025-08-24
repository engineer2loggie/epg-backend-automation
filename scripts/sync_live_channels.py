#!/usr/bin/env python3
import os, sys, re, json, ssl
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from hashlib import md5
from typing import Dict, Any, List, Tuple
from collections import Counter

# ----------------- Config / env -----------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

# Country set to ingest (ISO2, upper)
COUNTRIES = ["PR","DE","US","ES","MX","IT","IE","CA","AU","UK"]

SEARCH_BASE   = "https://iptv-org.github.io/?q=live%20country:{cc}"
CHANNELS_JSON = "https://iptv-org.github.io/api/channels.json"
STREAMS_JSON  = "https://iptv-org.github.io/api/streams.json"  # used ONLY to map url->channel id
ATTR_RE       = re.compile(r'([A-Za-z0-9_-]+)="([^"]*)"')
M3U_LINK_RE   = re.compile(r'href="([^"]*index\.m3u[^"]*)"', re.IGNORECASE)

# ----------------- Helpers ---------------------

def require_env():
    miss = []
    if not SUPABASE_URL: miss.append("SUPABASE_URL")
    if not SERVICE_KEY:  miss.append("SUPABASE_SERVICE_ROLE_KEY")
    if miss: raise SystemExit(f"Missing env: {', '.join(miss)}")
    try:
        print("ENV OK:", SUPABASE_URL.split("//",1)[1].split("/",1)[0])
    except Exception:
        print("ENV OK")

def fetch_text(url: str, timeout=90) -> str:
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent": "iptv-live-html/1.0"})
    with urlopen(req, context=ctx, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return resp.read().decode("utf-8", "ignore")

def fetch_json(url: str, timeout=90) -> Any:
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent": "iptv-live-html/1.0"})
    with urlopen(req, context=ctx, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.loads(resp.read().decode("utf-8", "ignore"))

def supabase_delete_by_countries(countries: List[str]) -> int:
    """
    DELETE existing rows for these countries so we don't accumulate junk.
    """
    import urllib.parse, urllib.request
    url = f"{SUPABASE_URL}/rest/v1/live_channels"
    # country=in.(PR,DE,...)  -> URL-encode the value inside ()
    in_list = ",".join(countries)
    params = {"country": f"in.({in_list})"}
    full = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url=full, method="DELETE",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Prefer": "return=representation"
        }
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        # Some PostgREST configs return 204 for DELETE; count may not be present.
        return 0

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

# ----------------- Playwright scraping ---------

def playlist_url_from_search(country: str) -> str | None:
    """
    Render the HTML search page and extract the injected index.m3u link.
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        print("Playwright import failed:", e)
        return None

    url = SEARCH_BASE.format(cc=country)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (LiveScraper/1.0)")
        try:
            page.goto(url, wait_until="networkidle", timeout=60_000)
            page.wait_for_timeout(1200)
            html = page.content()
        finally:
            browser.close()

    for href in M3U_LINK_RE.findall(html):
        if href.startswith("http"):
            return href
        return f"https://iptv-org.github.io{href}"
    return None

# ----------------- Parsing / mapping -----------

def parse_m3u(text: str) -> List[Dict[str, str]]:
    """
    → [{'tvg_id','tvg_name','tvg_logo','display','url'}, ...]
    """
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

# ----------------- Main -----------------------

def main():
    require_env()

    # Load authoritative catalog
    print("Fetching channels catalog…")
    catalog = fetch_json(CHANNELS_JSON)
    by_id: Dict[str, Dict[str, Any]] = {
        (c.get("id") or "").lower(): c
        for c in catalog if isinstance(c, dict) and c.get("id")
    }
    # Name index per country (fallback matching)
    name_key_to_id: Dict[Tuple[str,str], str] = {}
    for cid, meta in by_id.items():
        cc = (meta.get("country") or "").upper()
        nm = meta.get("name") or ""
        if cc and nm:
            name_key_to_id[(cc, norm_name(nm))] = cid

    # Build URL→channel-id mapping (for entries missing tvg-id)
    print("Fetching streams catalog (for url→channel mapping)…")
    streams = fetch_json(STREAMS_JSON)
    url_to_id: Dict[str, str] = {}
    for s in streams:
        url = (s.get("url") or "").strip()
        ch  = (s.get("channel") or "").strip().lower()
        if url and ch:
            url_to_id[url] = ch

    rows_by_id: Dict[str, Dict[str, Any]] = {}
    per_playlist_count: Dict[str, int] = {}
    kept_by_country: Counter = Counter()
    skipped_unmapped = 0
    resolved_catalog = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for cc in COUNTRIES:
        plist_url = playlist_url_from_search(cc)
        if not plist_url:
            # deterministic fallback if HTML shape changes
            plist_url = f"https://iptv-org.github.io/iptv/index.m3u?country={cc}&status=online"
            print(f"[{cc}] Fallback playlist:", plist_url)
        else:
            print(f"[{cc}] Playlist via HTML:", plist_url)

        try:
            m3u_text = fetch_text(plist_url)
        except Exception as e:
            print(f"[{cc}] ERROR fetching playlist: {e}")
            continue

        entries = parse_m3u(m3u_text)
        per_playlist_count[cc] = len(entries)
        print(f"[{cc}] Parsed {len(entries)} entries")

        for e in entries:
            tvg_id   = e["tvg_id"]
            tvg_name = e["tvg_name"]
            tvg_logo = e["tvg_logo"]
            display  = e["display"]
            url      = e["url"]

            # Resolve to catalog id: tvg-id → url map → name match
            channel_id = None
            if tvg_id and tvg_id in by_id:
                channel_id = tvg_id
            elif url in url_to_id and url_to_id[url] in by_id:
                channel_id = url_to_id[url]
            else:
                key = (cc, norm_name(tvg_name or display))
                if key in name_key_to_id:
                    channel_id = name_key_to_id[key]

            if not channel_id:
                skipped_unmapped += 1
                continue  # skip anything we can’t map to canonical id

            meta = by_id.get(channel_id, {})
            meta_cc = (meta.get("country") or "").upper()
            # Enforce that the channel actually belongs to the requested country
            if meta_cc != cc:
                continue

            icon_url = meta.get("logo") or (tvg_logo or None)
            display_name = meta.get("name") or tvg_name or display or channel_id

            # Keep first-seen stream per channel_id (one best row per channel)
            if channel_id not in rows_by_id:
                rows_by_id[channel_id] = {
                    "channel_id":   channel_id,
                    "display_name": display_name,
                    "country":      meta_cc,
                    "icon_url":     icon_url,
                    "stream_url":   url,
                    "check_time":   now_iso,
                    "source":       "iptv-org-html",
                    "updated_at":   now_iso,
                }
                kept_by_country[meta_cc] += 1
                resolved_catalog += 1

    final_rows = list(rows_by_id.values())
    print("per-playlist parsed:", per_playlist_count)
    print("final rows (deduped):", len(final_rows))
    print("kept by country:", dict(sorted(kept_by_country.items())))
    print("skipped (unmapped):", skipped_unmapped)

    if not final_rows:
        print("Nothing to upsert.")
        return 0

    # Clean then upsert
    print("Deleting existing rows for target countries…")
    supabase_delete_by_countries(COUNTRIES)

    print("Upserting canonical rows…")
    upsert_rows(final_rows)
    print("Done.")
    return 0

# Entry
if __name__ == "__main__":
    try:
        sys.exit(main())
    except (URLError, HTTPError) as e:
        print(f"Network error: {e}", file=sys.stderr); sys.exit(2)
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr); sys.exit(1)
