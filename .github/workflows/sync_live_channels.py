#!/usr/bin/env python3
import os, sys, json
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import ssl

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
COUNTRIES    = ["PR","DE","US","ES","MX","IT","IE","CA","AU","UK"]  # upper-case ISO2

CHANNELS_URL = "https://iptv-org.github.io/api/channels.json"
STREAMS_URL  = "https://iptv-org.github.io/api/streams.json"

def require_env():
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SERVICE_KEY:  missing.append("SUPABASE_SERVICE_KEY")
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")

def fetch_json(url: str):
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent":"live-scraper/1.0"})
    with urlopen(req, context=ctx, timeout=60) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.load(resp)

def pick_best_stream(streams):
    """Choose the best stream by resolution/bitrate/frame rate, fallback to first."""
    def score(s):
        w = s.get("width") or 0
        h = s.get("height") or 0
        br = s.get("bitrate") or 0
        fr = s.get("frame_rate") or 0
        return (w*h, br, fr)
    return sorted(streams, key=score, reverse=True)[0]

def upsert_rows(rows):
    import urllib.request
    url = f"{SUPABASE_URL}/rest/v1/live_channels?on_conflict=channel_id"
    data = json.dumps(rows).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
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

def main():
    require_env()
    print("Fetching IPTV-org channels/streams ...")
    channels = fetch_json(CHANNELS_URL)
    streams  = fetch_json(STREAMS_URL)

    ch_by_id = {c["id"]: c for c in channels if isinstance(c, dict) and "id" in c}

    online = [s for s in streams if s.get("status") == "online" and isinstance(s.get("channel"), str)]
    by_channel = {}
    for s in online:
        cid = s["channel"]
        by_channel.setdefault(cid, []).append(s)

    rows = []
    now_iso = datetime.now(timezone.utc).isoformat()
    target = set(COUNTRIES)
    for cid, slist in by_channel.items():
        ch = ch_by_id.get(cid)
        if not ch:
            continue
        country = (ch.get("country") or "").upper()
        if country not in target:
            continue
        best = pick_best_stream(slist)
        rows.append({
            "channel_id":     cid,
            "display_name":   ch.get("name") or cid,
            "country":        country,
            "icon_url":       ch.get("logo"),
            "stream_url":     best.get("url"),
            "check_time":     best.get("check_time") or now_iso,
            "source":         "iptv-org",
            "updated_at":     now_iso,
        })

    if not rows:
        print("No rows to upsert (no online streams for target countries).")
        return 0

    print(f"Upserting {len(rows)} rows to Supabase ...")
    upsert_rows(rows)
    print("Done.")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (URLError, HTTPError) as e:
        print(f"Network error: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr)
        sys.exit(1)
