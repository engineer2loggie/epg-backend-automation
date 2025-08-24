#!/usr/bin/env python3
import os, sys, re, json
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import ssl
from hashlib import md5
from typing import Dict, Any, List

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

# Target countries to fetch "live" playlists for
COUNTRIES    = ["PR","DE","US","ES","MX","IT","IE","CA","AU","UK"]  # ISO2 upper

# Live-only M3U endpoint (no browser needed)
M3U_URL_TMPL   = "https://iptv-org.github.io/iptv/index.m3u?country={cc}&status=online"
CHANNELS_JSON  = "https://iptv-org.github.io/api/channels.json"

# Parse attributes inside EXTINF (#EXTINF:-1 tvg-id="..." tvg-name="..." tvg-logo="...")
ATTR_RE = re.compile(r'([A-Za-z0-9_-]+)="([^"]*)"')

def require_env():
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SERVICE_KEY:  missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    if ".supabase.co" not in SUPABASE_URL:
        print(f"WARNING: SUPABASE_URL may be wrong (expected https://<id>.supabase.co), got: {SUPABASE_URL}")
    try:
        print("ENV OK for Supabase domain:", SUPABASE_URL.split("//",1)[1].split("/",1)[0])
    except Exception:
        print("ENV OK")

def fetch_text(url: str, timeout=90) -> str:
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent": "live-m3u-scraper/1.0"})
    with urlopen(req, context=ctx, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return resp.read().decode("utf-8", "ignore")

def fetch_json(url: str, timeout=90) -> Any:
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent": "live-m3u-scraper/1.0"})
    with urlopen(req, context=ctx, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.loads(resp.read().decode("utf-8", "ignore"))

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
    with urllib.request.urlopen(req, timeout=60) as resp:
        if resp.status not in (200,201,204):
            body = resp.read().decode("utf-8", "ignore")
            raise RuntimeError(f"Supabase upsert failed {resp.status}: {body}")

def parse_m3u(text: str) -> List[Dict[str, str]]:
    """
    Parse M3U into a list of dicts:
    { 'tvg_id', 'tvg_name', 'tvg_logo', 'display', 'url' }
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
            # advance to the next non-comment for the URL
            j = i + 1
            while j < len(lines) and lines[j].startswith("#"):
                j += 1
            url = lines[j] if j < len(lines) else None
            if url:
                out.append({
                    "tvg_id"  : (attrs.get("tvg-id") or "").strip(),
                    "tvg_name": (attrs.get("tvg-name") or "").strip(),
                    "tvg_logo": (attrs.get("tvg-logo") or "").strip(),
                    "display" : display.strip(),
                    "url"     : url.strip(),
                })
            i = j + 1
        else:
            i += 1
    return out

def main():
    require_env()

    # Load authoritative channel catalog once to resolve country/logo by tvg-id
    print("Fetching channels catalog …")
    channels = fetch_json(CHANNELS_JSON)
    # id -> dict with 'country','logo','name'
    ch_by_id: Dict[str, Dict[str, Any]] = {
        (c.get("id") or "").lower(): c for c in channels
        if isinstance(c, dict) and c.get("id")
    }
    print(f"channels.json loaded: {len(ch_by_id)} ids")

    # We’ll accumulate a single row per channel_id across all countries
    rows_by_id: Dict[str, Dict[str, Any]] = {}
    resolved_country = 0
    fallback_country = 0
    per_country_parsed: Dict[str, int] = {}

    for cc in COUNTRIES:
        url = M3U_URL_TMPL.format(cc=cc)
        print(f"[{cc}] Fetch:", url)
        try:
            m3u = fetch_text(url)
        except Exception as e:
            print(f"[{cc}] ERROR fetching playlist: {e}")
            continue

        entries = parse_m3u(m3u)
        per_country_parsed[cc] = len(entries)
        print(f"[{cc}] Parsed {len(entries)} entries")

        for e in entries:
            tvg_id   = (e["tvg_id"] or "").lower()
            tvg_name = e["tvg_name"]
            tvg_logo = e["tvg_logo"]
            display  = e["display"]
            stream   = e["url"]

            # Prefer tvg-id as stable key; otherwise hash name|url
            if tvg_id:
                channel_id = tvg_id
            else:
                base = (tvg_name or display or stream).strip()
                channel_id = md5((base + "|" + stream).encode("utf-8")).hexdigest()

            # Resolve country/logo from catalog if possible
            if tvg_id and tvg_id in ch_by_id:
                meta     = ch_by_id[tvg_id]
                country  = (meta.get("country") or cc).upper()
                icon_url = meta.get("logo") or (tvg_logo or None)
                resolved_country += 1
            else:
                country  = cc  # fallback to the loop country only when unknown
                icon_url = (tvg_logo or None)
                fallback_country +_
