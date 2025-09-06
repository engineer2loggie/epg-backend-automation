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
from urllib.parse import urlparse, parse_qs

import requests

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
    We don't verify signature; we just parse `exp`.
    """
    try:
        m = re.search(r"/([A-Za-z0-9_\-]+=*?)/manifest/", url)
        if not m:
            chunks = [c for c in url.split('/') if c.startswith("eyJ")]
            if not chunks:
                return None
            token_like = chunks[0]
        else:
            token_like = m.group(1)

        parts = token_like.split('.')
        payload_b64 = parts[1] if len(parts) >= 2 else token_like
        payload = json.loads(base64.urlsafe_b64decode(_b64url_pad(payload_b64)).decode('utf-8', 'ignore'))

        exp = payload.get("exp")
        if isinstance(exp, (int, float)):
            return datetime.fromtimestamp(int(exp), tz=timezone.utc)
        return None
    except Exception:
        return None

def seconds_left(expiry_utc: datetime) -> int:
    now = datetime.now(timezone.utc)
    return int((expiry_utc - now).total_seconds())

def fetch(session: requests.Session, url: str, is_json: bool = False, params: dict = None, timeout: int = 20):
    """Uses a requests.Session to make HTTP requests."""
    r = session.get(url, timeout=timeout, params=params)
    r.raise_for_status()
    return r.json() if is_json else r.text

def find_iframe_url(html: str) -> Optional[str]:
    """Finds the restream.io player URL from the iframe using regex."""
    m = re.search(r'<iframe src="(https?://player\.restream\.io/[^"]+)"', html)
    if m:
        return m.group(1)
    return None

def extract_token_from_iframe_url(iframe_url: str) -> Optional[str]:
    """Extracts the token from the iframe URL's query parameters."""
    parsed_url = urlparse(iframe_url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get('token', [None])[0]

# -----------------------------
# Supabase helpers (optional)
# -----------------------------

def supabase_update_stream(
    session: requests.Session,
    supabase_url: str,
    supabase_key: str,
    table: str,
    match_where: dict,
    new_stream_url: str,
) -> Tuple[bool, str]:
    """Updates a stream_url in Supabase."""
    try:
        if not supabase_url.endswith("/"):
            supabase_url += "/"
        postgrest_url = supabase_url + "rest/v1/" + table

        params = [(f"{k}", f"eq.{v}") for k, v in match_where.items()]
        body = {"stream_url": f"web:{new_stream_url}"}

        # Temporarily remove API-specific headers for Supabase request
        original_headers = session.headers.copy()
        session.headers.update({
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        })
        # Remove headers that are specific to the restream API
        session.headers.pop('authority', None)
        session.headers.pop('origin', None)


        r = session.patch(postgrest_url, params=params, json=body, timeout=20)
        r.raise_for_status()
        updated_rows = r.json()
        
        session.headers = original_headers # Restore original headers

        if not updated_rows:
             return False, "No rows matched your selector to update."
        
        return True, f"Updated {len(updated_rows)} row(s)."

    except requests.exceptions.RequestException as e:
        session.headers = original_headers # Restore original headers on error
        return False, f"Supabase update failed: {e}"

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Refresh TeleOnce Cloudflare Stream URL (graceful).")
    ap.add_argument("--page", required=True, help="The page that embeds the player (e.g. https://cdn.teleonce.com/en-vivo/)")
    ap.add_argument("--threshold", type=int, default=3*60*60, help="Seconds remaining below which we refresh (default 10800 = 3h)")
    ap.add_argument("--current", help="Current stream URL (optional).")
    ap.add_argument("--write", action="store_true", help="If set, write the refreshed URL back to Supabase.")
    ap.add_argument("--table", default="manual_tv_input", help="Supabase table to update.")
    ap.add_argument("--match-field", default="channel_name", help="Column used to match the row.")
    ap.add_argument("--match-value", default="Tele Once", help="Value used to match the row.")
    args = ap.parse_args()

    # 1) If we have a current URL, report its expiry
    if args.current:
        exp = decode_cloudflare_exp_from_url(args.current)
        if exp:
            left = seconds_left(exp)
            print(f"[info] current exp: {exp.strftime('%Y-%m-%d %H:%M:%S %Z')}  ({left:+d}s left)")
        else:
            print("[warn] Could not decode expiry from --current URL.")

    # 2) If current is still “healthy” (above threshold), exit gracefully
    if args.current and (exp := decode_cloudflare_exp_from_url(args.current)):
        if seconds_left(exp) > args.threshold:
            print(f"[info] current URL still above threshold. No refresh needed.")
            sys.exit(0)

    # Create a session for efficient, repeated requests.
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/102.0.0.0 Safari/537.36"
        )
    })

    try:
        # 1) Fetch the main page and find the iframe URL
        print(f"[info] 1/3: Fetching main page: {args.page}")
        session.headers.update({"Referer": args.page})
        html_main = fetch(session, args.page)
        iframe_url = find_iframe_url(html_main)
        if not iframe_url:
            print("[error] Could not find restream.io iframe on the main page.")
            sys.exit(0)
        print(f"[info] 1/3: Found iframe URL: {iframe_url}")

        # 2) Extract token from iframe URL
        token = extract_token_from_iframe_url(iframe_url)
        if not token:
            print("[error] Could not extract token from iframe URL.")
            sys.exit(0)
        print(f"[info] 2/3: Extracted token.")

        # 3) Build the final API url and call it with the correct headers and params
        # This information is based on the working example script.
        session.headers.update({
            'authority': 'player-backend.restream.io',
            'origin': 'https://player.restream.io',
            'referer': 'https://player.restream.io/',
        })
        
        api_url = f"https://player-backend.restream.io/public/videos/{token}"
        params = {'instant': 'true'}

        print(f"[info] 3/3: Calling final API: {api_url}")
        
        api_data = fetch(session, api_url, is_json=True, params=params)
        
        new_m3u8 = api_data.get("hlsUrl")
        if not new_m3u8:
            print(f"[error] Could not find 'hlsUrl' in API response. Full response: {api_data}")
            sys.exit(0)
        print(f"[info] Success! Found M3U8 URL.")

    except requests.exceptions.RequestException as e:
        print(f"[error] Scraping process failed: {e}")
        sys.exit(0)

    # 4) Compare vs current
    if args.current and new_m3u8 == args.current:
        print("[info] Found same URL as current. Nothing to update.")
        sys.exit(0)

    # 5) Decode expiry for the new URL for logging
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
        session=session,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table=args.table,
        match_where={args.match_field: args.match_value},
        new_stream_url=new_m3u8,
    )
    if ok:
        print(f"[ok] {msg}")
    else:
        print(f"[error] {msg}")
    
    # Gracefully exit 0 even on supabase failure
    sys.exit(0)

if __name__ == "__main__":
    main()

