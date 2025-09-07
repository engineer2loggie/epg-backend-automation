#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Optional, Tuple

import requests

# -----------------------------
# Helpers
# -----------------------------

def fetch(session: requests.Session, url: str, is_json: bool = False, params: dict = None, timeout: int = 20):
    """Uses a requests.Session to make HTTP requests."""
    r = session.get(url, timeout=timeout, params=params)
    r.raise_for_status()
    return r.json() if is_json else r.text

def find_iframe_url(html: str) -> Optional[str]:
    """Find the restream.io player URL from the iframe using regex."""
    m = re.search(r'<iframe src="(https?://player\.restream\.io/[^"]+)"', html)
    if m:
        return m.group(1)
    return None

# -----------------------------
# Supabase helpers
# -----------------------------

def supabase_get_stream(
    session: requests.Session,
    supabase_url: str,
    supabase_key: str,
    table: str,
    match_where: dict,
) -> Tuple[Optional[str], str]:
    """
    Fetches the current `stream_url` from a Supabase table.
    Returns (stream_url, message).
    """
    try:
        if not supabase_url.endswith("/"):
            supabase_url += "/"
        postgrest_url = supabase_url + "rest/v1/" + table

        params = [(f"{k}", f"eq.{v}") for k, v in match_where.items()]
        params.append(("select", "stream_url"))
        params.append(("limit", "1"))
        
        original_headers = session.headers.copy()
        session.headers.clear()
        session.headers.update({
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        })

        r = session.get(postgrest_url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        
        session.headers = original_headers

        if not data:
            return None, "No row matched your selector."
        
        url = data[0].get("stream_url")
        # Strip "web:" prefix for comparison
        if url and url.startswith("web:"):
            url = url[4:]
            
        return url, "Successfully fetched current URL."

    except requests.exceptions.RequestException as e:
        session.headers = original_headers
        return None, f"Supabase fetch failed: {e}"


def supabase_update_stream(
    session: requests.Session,
    supabase_url: str,
    supabase_key: str,
    table: str,
    match_where: dict,
    new_stream_url: str,
) -> Tuple[bool, str]:
    """Updates a stream_url in Supabase, adding the 'web:' prefix."""
    try:
        if not supabase_url.endswith("/"):
            supabase_url += "/"
        postgrest_url = supabase_url + "rest/v1/" + table

        params = [(f"{k}", f"eq.{v}") for k, v in match_where.items()]
        # Add the "web:" prefix back to maintain DB format
        body = {"stream_url": f"web:{new_stream_url}"}

        original_headers = session.headers.copy()
        session.headers.clear()
        session.headers.update({
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        })

        r = session.patch(postgrest_url, params=params, json=body, timeout=20)
        r.raise_for_status()
        updated_rows = r.json()
        
        session.headers = original_headers

        if not updated_rows:
             return False, "No rows matched your selector to update."
        
        return True, f"Updated {len(updated_rows)} row(s)."

    except requests.exceptions.RequestException as e:
        session.headers = original_headers
        return False, f"Supabase update failed: {e}"

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Refresh TeleOnce iframe URL.")
    ap.add_argument("--page", required=True, help="The page that embeds the player (e.g. https://cdn.teleonce.com/en-vivo/)")
    ap.add_argument("--write", action="store_true", help="If set, write the refreshed URL back to Supabase.")
    ap.add_argument("--table", default="manual_tv_input", help="Supabase table to update.")
    ap.add_argument("--match-field", default="channel_name", help="Column used to match the row.")
    ap.add_argument("--match-value", default="Tele Once", help="Value used to match the row.")
    args = ap.parse_args()

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/102.0.0.0 Safari/537.36"
        )
    })

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        print("[error] SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY are not set.")
        sys.exit(1)
        
    current_url, msg = supabase_get_stream(
        session=session,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table=args.table,
        match_where={args.match_field: args.match_value},
    )
    print(f"[info] {msg}")
    if current_url:
        print(f"[info] Current DB URL (cleaned): {current_url}")
    else:
        print("[warn] Could not fetch current URL from DB.")

    new_iframe_url = None
    try:
        print(f"[info] Fetching main page: {args.page}")
        session.headers.update({"Referer": args.page})
        html_main = fetch(session, args.page)

        new_iframe_url = find_iframe_url(html_main)
        if not new_iframe_url:
            print("[error] Could not find restream.io iframe on the main page.")
            sys.exit(0)
        print(f"[info] Success! Found scraped URL: {new_iframe_url}")

    except requests.exceptions.RequestException as e:
        print(f"[error] Scraping process failed: {e}")
        sys.exit(0)

    if current_url and new_iframe_url == current_url:
        print("[info] Scraped URL is the same as DB URL. Nothing to update.")
        sys.exit(0)
    
    print("[info] New or changed URL detected.")

    if not args.write:
        print("[info] Dry-run (no --write). New URL is:")
        print(new_iframe_url)
        sys.exit(0)

    ok, msg = supabase_update_stream(
        session=session,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table=args.table,
        match_where={args.match_field: args.match_value},
        new_stream_url=new_iframe_url,
    )
    if ok:
        print(f"[ok] {msg}")
    else:
        print(f"[error] {msg}")
    
    sys.exit(0)

if __name__ == "__main__":
    main()

