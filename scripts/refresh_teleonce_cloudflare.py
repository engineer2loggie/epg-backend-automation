#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

# -----------------------------
# Helpers
# -----------------------------

def _b64url_pad(s: str) -> str:
    # base64url strings may be missing padding; add it.
    pad = '=' * (-len(s) % 4)
    return s + pad

def decode_cloudflare_exp_from_url(url: str) -> Optional[datetime]:
    """
    Extract JWT segment from Cloudflare Stream URL path and read exp (unix epoch).
    Example path piece starts with 'eyJ...' (header.payload.signature style).
    We don't verify signature; we just parse `exp`.
    """
    try:
        # Find the first base64url-looking chunk (payload is the 2nd part of a JWT)
        # Many CF Stream URLs look like .../<JWT>/manifest/video.m3u8
        # JWT = header.payload.signature (dot separated)
        m = re.search(r"/([A-Za-z0-9_\-]+=*?)/manifest/", url)
        if not m:
            # Fallback: find any eyJ... then expand to header.payload.signature if present in path
            chunks = [c for c in url.split('/') if c.startswith("eyJ")]
            if not chunks:
                return None
            token_like = chunks[0]
        else:
            token_like = m.group(1)

        # If it's a full JWT (with dots), use the payload; otherwise try as a single compact token
        parts = token_like.split('.')
        if len(parts) >= 2:
            payload_b64 = parts[1]
            payload = json.loads(base64.urlsafe_b64decode(_b64url_pad(payload_b64)).decode('utf-8', 'ignore'))
        else:
            # Some providers cram everything in one segment; try to decode as JSON
            payload = json.loads(base64.urlsafe_b64decode(_b64url_pad(token_like)).decode('utf-8', 'ignore'))

        exp = payload.get("exp")
        if isinstance(exp, (int, float)):
            return datetime.fromtimestamp(int(exp), tz=timezone.utc)
        return None
    except Exception:
        return None

def seconds_left(expiry_utc: datetime) -> int:
    now = datetime.now(timezone.utc)
    return int((expiry_utc - now).total_seconds())

def fetch(url: str, is_json: bool = False, timeout: int = 20):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*" if is_json else "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://cdn.teleonce.com/en-vivo/",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json() if is_json else r.text

def find_iframe_url(html: str) -> Optional[str]:
    """Finds the restream.io player URL from the iframe."""
    m = re.search(r'<iframe src="(https?://player\.restream\.io/[^"]+)"', html)
    if m:
        return m.group(1)
    return None

def find_js_url(html: str, base_url: str) -> Optional[str]:
    """Finds the relative JS file URL from a script tag and makes it absolute."""
    m = re.search(r'<script src="(/[^"]+\.js)"', html)
    if not m:
        return None
    relative_path = m.group(1)
    return urljoin(base_url, relative_path)

def find_api_path_in_js(js_code: str) -> Optional[str]:
    """
    Finds the API endpoint URL in the javascript code.
    This searches for the full URL containing the API path.
    """
    m = re.search(r'"(https?://[^"]*/api/v2/player/info)"', js_code)
    if m:
        return m.group(1)  # Return the full URL
    return None

def extract_token(iframe_url: str) -> Optional[str]:
    """Extracts the token from the iframe URL's query parameters."""
    parsed_url = urlparse(iframe_url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get('token', [None])[0]


# -----------------------------
# Supabase helpers (optional)
# -----------------------------

def supabase_update_stream(
    supabase_url: str,
    supabase_key: str,
    table: str,
    match_where: dict,
    new_stream_url: str,
) -> Tuple[bool, str]:
    """
    Update `stream_url` in `table` with `web:<new_stream_url>` using Supabase REST (PostgREST).
    match_where: dict of equality filters to identify row(s), e.g. {"channel_name": "Tele Once"}
    Returns (ok, message).
    """
    try:
        if not supabase_url.endswith("/"):
            supabase_url += "/"
        postgrest = supabase_url + "rest/v1/" + table

        params = [(f"{k}", f"eq.{v}") for k, v in match_where.items()]
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        body = {"stream_url": f"web:{new_stream_url}"}

        r = requests.patch(postgrest, headers=headers, params=params, json=body, timeout=20)
        r.raise_for_status()
        updated_rows = r.json()
        
        if not updated_rows:
             return False, "No rows matched your selector to update."
        
        return True, f"Updated {len(updated_rows)} row(s)."

    except Exception as e:
        return False, f"Supabase update failed: {e}"

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Refresh TeleOnce Cloudflare Stream URL (graceful).")
    ap.add_argument("--page", required=True, help="The page that embeds the player (e.g. https://cdn.teleonce.com/en-vivo/)")
    ap.add_argument("--threshold", type=int, default=3*60*60, help="Seconds remaining below which we refresh (default 10800 = 3h)")
    ap.add_argument("--current", help="Current stream URL (optional). If omitted, script will just try to fetch a new one and report.")
    ap.add_argument("--write", action="store_true", help="If set, write the refreshed URL back to Supabase.")
    ap.add_argument("--table", default="manual_tv_input", help="Supabase table to update (default manual_tv_input)")
    ap.add_argument("--match-field", default="channel_name", help="Column used to match the row (default channel_name)")
    ap.add_argument("--match-value", default="Tele Once", help="Value used to match the row (default 'Tele Once')")
    args = ap.parse_args()

    # 1) If we have a current URL, report its expiry (if any)
    if args.current:
        exp = decode_cloudflare_exp_from_url(args.current)
        if exp:
            left = seconds_left(exp)
            print(f"[info] current exp: {exp.strftime('%Y-%m-%d %H:%M:%S %Z')}  ({left:+d}s left)")
        else:
            print("[warn] Could not decode expiry from --current URL.")

    # 2) If current is still “healthy” (above threshold), we can exit gracefully
    if args.current:
        exp = decode_cloudflare_exp_from_url(args.current)
        if exp:
            left = seconds_left(exp)
            if left > args.threshold:
                print(f"[info] current URL still above threshold ({left}s left). No refresh needed.")
                sys.exit(0)

    # --- NEW MULTI-STEP SCRAPING LOGIC ---
    try:
        # 3a) Fetch the main page and find the iframe URL
        print(f"[info] 1/5: Fetching main page: {args.page}")
        html_main = fetch(args.page)
        iframe_url = find_iframe_url(html_main)
        if not iframe_url:
            print("[error] Could not find restream.io iframe on the main page.")
            sys.exit(0)
        print(f"[info] 1/5: Found iframe URL: {iframe_url}")

        # 3b) Fetch the iframe's HTML to find the script URL
        print(f"[info] 2/5: Fetching iframe content...")
        html_iframe = fetch(iframe_url)
        js_url = find_js_url(html_iframe, iframe_url)
        if not js_url:
            print("[error] Could not find JS file URL in iframe HTML.")
            sys.exit(0)
        print(f"[info] 2/5: Found JS file: {js_url}")
        
        # 3c) Fetch the JS file to find the API path
        print(f"[info] 3/5: Fetching JS content...")
        js_code = fetch(js_url)
        api_url = find_api_path_in_js(js_code)
        if not api_url:
            print("[error] Could not find API URL in JS file.")
            sys.exit(0)
        print(f"[info] 3/5: Found API URL: {api_url}")
        
        # 3d) Build the final API url and call it
        token = extract_token(iframe_url)
        if not token:
            print("[error] Could not extract token from iframe URL.")
            sys.exit(0)
        
        full_api_url = f"{api_url}?token={token}"
        print(f"[info] 4/5: Calling final API: {full_api_url}")
        
        api_data = fetch(full_api_url, is_json=True)
        
        # 3e) Extract the m3u8 url from the API JSON response
        new_m3u8 = api_data.get("hlsUrl")
        if not new_m3u8:
            print(f"[error] Could not find 'hlsUrl' in API response. Full response: {api_data}")
            sys.exit(0)
        print(f"[info] 5/5: Success! Found M3U8 URL.")

    except Exception as e:
        print(f"[error] Scraping process failed: {e}")
        sys.exit(0)


    # 4) Compare vs current (if provided)
    if args.current and new_m3u8 == args.current:
        print("[info] Found same URL as current. Nothing to update.")
        sys.exit(0)

    # 5) Decode expiry for the new URL (if present), just for logging
    new_exp = decode_cloudflare_exp_from_url(new_m3u8)
    if new_exp:
        print(f"[info] new exp: {new_exp.strftime('%Y-%m-%d %H:%M:%S %Z')}  ({seconds_left(new_exp)}s left)")
    else:
        print("[warn] Could not decode expiry from new URL (may still be valid).")

    if not args.write:
        print("[info] Dry-run (no --write). New URL detected:")
        print(new_m3u8)
        sys.exit(0)

    # 6) Write to Supabase if requested
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        print("[error] --write given, but SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY are not set.")
        sys.exit(1)

    ok, msg = supabase_update_stream(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table=args.table,
        match_where={args.match_field: args.match_value},
        new_stream_url=new_m3u8,
    )
    if ok:
        print(f"[ok] {msg}")
        sys.exit(0)
    else:
        print(f"[error] {msg}")
        sys.exit(0)

if __name__ == "__main__":
    main()


