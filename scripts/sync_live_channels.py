#!/usr/bin/env python3
import os, sys, re, json
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import ssl
from hashlib import md5

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
COUNTRIES    = ["PR","DE","US","ES","MX","IT","IE","CA","AU","UK"]

M3U_URL_TMPL = "https://iptv-org.github.io/iptv/index.m3u?country={cc}&status=online"
ATTR_RE      = re.compile(r'([A-Za-z0-9_-]+)="([^"]*)"')

def require_env():
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SERVICE_KEY:  missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    if ".supabase.co" not in SUPABASE_URL:
        print(f"WARNING: SUPABASE_URL may be wrong: {SUPABASE_URL}")
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

def upsert_rows(rows):
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

def parse_m3u(text: str, country: str):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXTINF"):
            try:
                header, display = line.split(",", 1)
            except ValueError:
                header, display = line, ""
            attrs = dict(ATTR_RE.findall(header))
            tvg_id   = (attrs.get("tvg-id") or "").strip()
            tvg_name = (attrs.get("tvg-name") or "").strip()
            tvg_logo = (attrs.get("tvg-logo") or "").strip()

            # next non-comment line is the URL
            j = i + 1
            while j < len(lines) and lines[j].startswith("#"): j += 1
            stream_url = lines[j] if j < len(lines) else None

            if stream_url:
                if tvg_id:
                    channel_id = tvg_id.lower()
                else:
                    base = (tvg_name or display or stream_url).strip()
                    channel_id = md5((base + "|" + stream_url).encode("utf-8")).hexdigest()
                display_name = (tvg_name or display).strip() or channel_id
                rows.append({
                    "channel_id":   channel_id,
                    "display_name": display_name,
                    "country":      country.upper(),
                    "icon_url":     tvg_logo or None,
                    "stream_url":   stream_url,
                    "check_time":   now_iso,
                    "source":       "iptv-org-live-m3u",
                    "updated_at":   now_iso,
                })
            i = j + 1
        else:
            i += 1
    return rows

def main():
    require_env()
    total = 0
    per_country = {}
    for cc in COUNTRIES:
        url = M3U_URL_TMPL.format(cc=cc)
        print(f"[{cc}] Fetch:", url)
        try:
            m3u = fetch_text(url)
        except Exception as e:
            print(f"[{cc}] ERROR fetching playlist: {e}")
            continue
        rows = parse_m3u(m3u, cc)
        per_country[cc] = len(rows)
        total += len(rows)
        if rows:
            print(f"[{cc}] Parsed {len(rows)} rows → upserting …")
            try:
                upsert_rows(rows)
                print(f"[{cc}] Upserted {len(rows)} rows")
            except Exception as e:
                print(f"[{cc}] ERROR during upsert: {e}")
        else:
            print(f"[{cc}] No rows parsed.")
    print("kept per country:", per_country)
    print("Total upserted rows:", total)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (URLError, HTTPError) as e:
        print(f"Network error: {e}", file=sys.stderr); sys.exit(2)
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr); sys.exit(1)
