#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import time
import json
import base64
import argparse
from typing import Optional

import requests
from supabase import create_client, Client


# ------------- config -------------
DEFAULT_PAGE_URL = "https://cdn.teleonce.com/en-vivo/"
# How close to expiry before we refresh (seconds)
DEFAULT_REFRESH_THRESHOLD = 3 * 60 * 60  # 3 hours

# Which manual_tv_input row to update. You can match ANY of these safely.
MATCH_CHANNEL_NAME = "Tele Once"
MATCH_COUNTRY_CODE = "PR"
MATCH_PROGRAMME_SOURCE_LINK_CONTAINS = "ontvtonight.com/guide/listings/channel/69025491"

# ------------- utils -------------
def _jwt_payload(jwt: str) -> Optional[dict]:
    """Return decoded JWT payload or None."""
    parts = jwt.split(".")
    if len(parts) < 2:
        return None
    b64 = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(b64))
    except Exception:
        return None


def get_expiry_from_manifest_url(url: str) -> Optional[int]:
    """
    If the HLS manifest URL contains a JWT path segment (Cloudflare 'customer-...' token),
    decode it and return exp (epoch seconds). Otherwise None.
    """
    # Example:
    # https://customer-xxxx.cloudflarestream.com/<JWT>/manifest/video.m3u8
    m = re.search(r"https://customer-[^/]+/([^/]+)/manifest/video\.m3u8", url)
    if not m:
        return None
    jwt = m.group(1)
    payload = _jwt_payload(jwt)
    if not payload:
        return None
    return int(payload.get("exp", 0)) or None


def scrape_manifest_from_page(page_url: str) -> Optional[str]:
    """
    Fetches the TeleOnce 'en vivo' page and extracts an HLS manifest URL.
    Handles common Cloudflare Stream patterns.
    """
    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
    r = requests.get(page_url, headers={"User-Agent": UA}, timeout=20)
    r.raise_for_status()
    html = r.text

    # Pattern 1: customer-* cloudflarestream with JWT
    p1 = re.search(
        r"https://customer-[^\"']+/[^\"']+/manifest/video\.m3u8", html, re.IGNORECASE
    )
    if p1:
        return p1.group(0)

    # Pattern 2: videodelivery.net UUID-style
    p2 = re.search(
        r"https://(?:videodelivery|cloudflarestream)\.net/[0-9a-fA-F-]{8,}/manifest/video\.m3u8",
        html,
        re.IGNORECASE,
    )
    if p2:
        return p2.group(0)

    # Pattern 3: iframe or player JS that includes manifest/video.m3u8
    p3 = re.search(
        r"https?://[^\"']+/manifest/video\.m3u8", html, re.IGNORECASE
    )
    if p3:
        return p3.group(0)

    return None


def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required env vars.")
    return create_client(url, key)


def load_current_stream_row(sb: Client) -> Optional[dict]:
    """
    Grab the Tele Once row from manual_tv_input by a few stable identifiers.
    """
    q = sb.table("manual_tv_input").select("*")
    # Apply filters gently; some deployments may not have all fields.
    q = q.ilike("channel_name", MATCH_CHANNEL_NAME)
    q = q.eq("country_code", MATCH_COUNTRY_CODE)
    q = q.ilike("programme_source_link", f"%{MATCH_PROGRAMME_SOURCE_LINK_CONTAINS}%")
    res = q.limit(1).execute()
    rows = res.data or []
    return rows[0] if rows else None


def update_stream_url(sb: Client, row_id: int, new_url: str):
    """
    Write back the 'web:'-prefixed URL to stream_url.
    """
    payload = {"id": row_id, "stream_url": f"web:{new_url}"}
    sb.table("manual_tv_input").update(payload).eq("id", row_id).execute()


def maybe_refresh(page_url: str, threshold_s: int) -> bool:
    """
    True if a refresh is needed (either URL expiring soon or unparsable).
    """
    sb = get_supabase()
    row = load_current_stream_row(sb)
    if not row:
        print("[warn] Tele Once row not found.")
        return False

    current = (row.get("stream_url") or "").strip()
    if current.lower().startswith("web:"):
        current = current[4:]

    exp = get_expiry_from_manifest_url(current)
    now = int(time.time())
    if exp:
        secs_left = exp - now
        print(f"[info] current exp: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(exp))}  ({secs_left}s left)")
        if secs_left > threshold_s:
            print("[info] not near expiry; skip refresh.")
            return False
    else:
        print("[info] current URL has no decodable JWT; will refresh.")

    # Fetch a new manifest
    fresh = scrape_manifest_from_page(page_url)
    if not fresh:
        print("[error] could not find a new manifest on the page.")
        return False

    # If identical, we’re done.
    if fresh == current:
        print("[info] page returned same manifest; no update necessary.")
        return False

    # Save
    row_id = row.get("id")
    if not isinstance(row_id, int):
        raise RuntimeError("manual_tv_input must have numeric 'id' as primary key.")
    update_stream_url(sb, row_id, fresh)
    print(f"[ok] updated stream_url to web:{fresh}")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--page", default=DEFAULT_PAGE_URL, help="Page that hosts TeleOnce live player.")
    ap.add_argument("--threshold", type=int, default=DEFAULT_REFRESH_THRESHOLD, help="Refresh if expiry is within this many seconds.")
    args = ap.parse_args()
    maybe_refresh(args.page, args.threshold)


if __name__ == "__main__":
    main()
