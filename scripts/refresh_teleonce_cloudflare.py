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

def fetch_html(url: str, timeout: int = 20) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def find_m3u8_in_html(html: str, base_url: str) -> Optional[str]:
    """
    Try several strategies:
    1) <source type="application/x-mpegURL" src="...m3u8">
    2) any tag with src/href endswith .m3u8
    3) raw regex of absolute .m3u8 URLs
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) canonical <source type="application/x-mpegURL" src="...">
    for s in soup.find_all("source"):
        t = (s.get("type") or "").lower()
        src = s.get("src") or ""
        if "mpegurl" in t and ".m3u8" in src.lower():
            return src

    # 2) any tag with src/href containing .m3u8
    for tag in soup.find_all(True):
        for attr in ("src", "href", "data-src"):
            v = tag.get(attr)
            if v and ".m3u8" in v.lower():
                return v

    # 3) absolute URLs in the text
    m = re.search(r"https?://[^\s'\"<>]+\.m3u8[^\s'\"<>]*", html, re.IGNORECASE)
    if m:
        return m.group(0)

    return None

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

        # Build the ?filters for eq each field
        params = []
        for k, v in match_where.items():
            # PostgREST eq filter
            params.append((f"{k}", f"eq.{v}"))
        params.append(("select", "id,stream_url"))
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

        # 1) fetch matching rows
        r = requests.get(postgrest, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if not data:
            return False, "No rows matched your selector."

        # 2) update those rows
        patch_params = [(f"{k}", f"eq.{v}") for k, v in match_where.items()]
        body = {"stream_url": f"web:{new_stream_url}"}

        r2 = requests.patch(postgrest, headers=headers, params=patch_params, json=body, timeout=20)
        r2.raise_for_status()
        return True, f"Updated {len(data)} row(s)."

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

    # 3) Fetch the page and try to parse a fresh m3u8
    try:
        html = fetch_html(args.page)
    except Exception as e:
        # Graceful exit; do not fail the whole workflow if page is temporarily down
        print(f"[error] failed to fetch {args.page}: {e}")
        sys.exit(0)

    new_m3u8 = find_m3u8_in_html(html, args.page)
    if not new_m3u8:
        print("[info] No .m3u8 found on the page right now. Nothing to update.")
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
        # still exit 0 to be non-disruptive if you prefer:
        sys.exit(0)

if __name__ == "__main__":
    main()
