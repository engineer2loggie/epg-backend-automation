#!/usr/bin/env python3
import os, sys, json
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import ssl

# --- Config / env -------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
COUNTRIES    = ["PR","DE","US","ES","MX","IT","IE","CA","AU","UK"]  # ISO-2, upper

CHANNELS_URL = "https://iptv-org.github.io/api/channels.json"
STREAMS_URL  = "https://iptv-org.github.io/api/streams.json"

# --- Helpers ------------------------------------------------------------------

def require_env():
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SERVICE_KEY:  missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    # Non-fatal warning if URL looks odd
    if ".supabase.co" not in SUPABASE_URL:
        print(f"WARNING: SUPABASE_URL may be wrong (expected https://<id>.supabase.co), got: {SUPABASE_URL}")

    # Print only the domain (safe)
    try:
        domain = SUPABASE_URL.split("//",1)[1].split("/",1)[0]
        print("ENV OK for Supabase domain:", domain)
    except Exception:
        print("ENV OK")

def fetch_json(url: str):
    ctx = ssl.create_default_context()
    req = Request(url, headers={"User-Agent":"live-scraper/1.0"})
    with urlopen(req, context=ctx, timeout=60) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.load(resp)

def pick_best_stream(streams):
    """
    Choose a 'best' stream by resolution/bitrate/framerate; fallback to first.
    """
    def score(s):
        w  = s.get("width") or 0
        h  = s.get("height") or 0
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

# --- Main ---------------------------------------------------------------------

def main():
    require_env()

    print("Fetching IPTV-org channels/streams ...")
    channels = fetch_json(CHANNELS_URL)
    streams  = fetch_json(STREAMS_URL)
    print(f"channels.json: {len(channels)} items")
    print(f"streams.json : {len(streams)} items")

    # id -> channel record
    ch_by_id = {c["id"]: c for c in channels if isinstance(c, dict) and "id" in c}

    # Online streams only
    online = [s for s in streams if s.get("status") == "online" and isinstance(s.get("channel"), str)]
    print(f"online streams: {len(online)}")

    # Group streams by channel id
    by_channel = {}
    for s in online:
        cid = s["channel"]
        by_channel.setdefault(cid, []).append(s)

    # Build rows for target countries
    rows = []
    now_iso = datetime.now(timezone.utc).isoformat()
    target = set(COUNTRIES)
    kept_counts = {cc: 0 for cc in COUNTRIES}

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
        kept_counts[country] = kept_counts.get(country, 0) + 1

    print("kept per country:", kept_counts)
    print("total rows to upsert:", len(rows))

    if not rows:
        print("No rows to upsert (no online streams for target countries).")
        return 0

    print("Upserting to Supabase …")
    upsert_rows(rows)
    print("Done.")
    return 0

# Entry
if __name__ == "__main__":
    try:
        sys.exit(main())
    except (URLError, HTTPError) as e:
        print(f"Network error: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr)
        sys.exit(1)
