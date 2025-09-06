#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# ---------------- Config defaults ----------------
DEFAULT_PAGE_URL = "https://cdn.teleonce.com/en-vivo/"
DEFAULT_TABLE = "manual_tv_input"
DEFAULT_SOURCE_LINK = "https://cdn.teleonce.com/en-vivo/"
DEFAULT_HEADER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# ---------------- Utilities ----------------
def b64url_decode(data: str) -> bytes:
    pad = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + pad)

def jwt_exp_from_url(url: str) -> Optional[int]:
    """
    Cloudflare Stream URLs embed a JWT path segment. We decode payload (2nd part)
    and return 'exp' (unix seconds) if present.
    """
    try:
        # .../<JWT>/manifest/video.m3u8
        parts = urlparse(url).path.split("/")
        # find the first token that looks like a JWT (has 3 dot parts)
        for seg in parts:
            if seg.count(".") == 2:
                header_b64, payload_b64, _sig = seg.split(".")
                payload = json.loads(b64url_decode(payload_b64).decode("utf-8"))
                exp = payload.get("exp")
                if isinstance(exp, int):
                    return exp
        return None
    except Exception:
        return None

def seconds_left_from_exp(exp: int) -> int:
    now = int(datetime.now(timezone.utc).timestamp())
    return exp - now

def http_get(url: str, *, referer: Optional[str] = None, timeout: int = 20) -> str:
    headers = {
        "User-Agent": DEFAULT_HEADER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if referer:
        headers["Referer"] = referer
        headers["Origin"] = referer.rstrip("/")
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

_M3U8_RE = re.compile(r'https?://[^\s\'"]+?\.m3u8[^\s\'"]*', re.IGNORECASE)

def extract_manifests(html: str, base_url: str) -> List[str]:
    """
    Priority:
      1) <source type="application/x-mpegURL" src="…">
      2) Any <source src="…m3u8">
      3) Regex fallback inside HTML/script
    """
    out: List[str] = []
    soup = BeautifulSoup(html, "html.parser")

    # 1) exact type match
    for src in soup.select('source[type="application/x-mpegURL"][src]'):
        url = src.get("src", "").strip()
        if url:
            out.append(url if url.startswith("http") else urljoin(base_url, url))

    # 2) any source ending in .m3u8
    for src in soup.select("source[src]"):
        url = (src.get("src") or "").strip()
        if ".m3u8" in url.lower():
            out.append(url if url.startswith("http") else urljoin(base_url, url))

    # 3) regex fallback over full HTML
    for m in _M3U8_RE.finditer(html):
        out.append(m.group(0))

    # de-dupe preserving order
    seen = set()
    unique = []
    for u in out:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique

@dataclass
class MatchRow:
    id: int
    channel_name: Optional[str]
    programme_source_link: Optional[str]
    stream_url: Optional[str]

def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY env vars are required")
    return create_client(url, key)

def fetch_target_row(
    sb: Client,
    table: str,
    *, source_link: Optional[str],
    channel_name: Optional[str]
) -> Optional[MatchRow]:
    if source_link:
        res = sb.table(table).select("*").eq("programme_source_link", source_link).limit(1).execute()
        data = res.data or []
        if data:
            r = data[0]
            return MatchRow(
                id=r.get("id"),  # assumes integer PK column "id"
                channel_name=r.get("channel_name"),
                programme_source_link=r.get("programme_source_link"),
                stream_url=r.get("stream_url"),
            )
    if channel_name:
        res = sb.table(table).select("*").eq("channel_name", channel_name).limit(1).execute()
        data = res.data or []
        if data:
            r = data[0]
            return MatchRow(
                id=r.get("id"),
                channel_name=r.get("channel_name"),
                programme_source_link=r.get("programme_source_link"),
                stream_url=r.get("stream_url"),
            )
    return None

def update_stream_url(sb: Client, table: str, row_id: int, new_url: str):
    payload = {"stream_url": f"web:{new_url}"}
    sb.table(table).update(payload).eq("id", row_id).execute()

# ---------------- Main flow ----------------
def main():
    ap = argparse.ArgumentParser(description="Refresh TeleOnce Cloudflare Stream HLS URL.")
    ap.add_argument("--page", default=DEFAULT_PAGE_URL, help="Landing page to scrape for the new HLS URL.")
    ap.add_argument("--table", default=DEFAULT_TABLE, help="Supabase table name.")
    ap.add_argument("--source-link", default=DEFAULT_SOURCE_LINK, help="Row selector: programme_source_link.")
    ap.add_argument("--channel-name", default=None, help="Alternative row selector: channel_name if no source-link match.")
    ap.add_argument("--threshold-seconds", type=int, default=3 * 60 * 60, help="Refresh if seconds left < threshold.")
    ap.add_argument("--force", action="store_true", help="Force refresh regardless of current expiry.")
    args = ap.parse_args()

    sb = get_supabase()

    row = fetch_target_row(sb, args.table, source_link=args.source_link, channel_name=args.channel_name)
    if not row:
        print("[error] No matching row found in Supabase.", file=sys.stderr)
        sys.exit(2)

    current = (row.stream_url or "").strip()
    if current.lower().startswith("web:"):
        current = current[4:]

    # 1) Check current expiry if it looks like a Cloudflare Stream JWT URL
    seconds_left = None
    exp = jwt_exp_from_url(current) if current.startswith("http") else None
    if exp:
        seconds_left = seconds_left_from_exp(exp)
        dt_exp = datetime.fromtimestamp(exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
        print(f"[info] current exp: {dt_exp}  ({seconds_left}s left)")
    else:
        print("[info] current URL has no decodable exp; will treat as expired.")

    need_refresh = args.force or seconds_left is None or seconds_left < args.threshold_seconds
    if not need_refresh:
        print("[info] Token still fresh; no update needed.")
        return

    # 2) Scrape the page for a new manifest (look for <source type='application/x-mpegURL'> first)
    try:
        html = http_get(args.page, referer=args.page)
    except Exception as e:
        print(f"[error] failed to fetch page: {e}", file=sys.stderr)
        sys.exit(1)

    manifests = extract_manifests(html, base_url=args.page)
    if not manifests:
        print("[error] could not find a new manifest on the page.", file=sys.stderr)
        sys.exit(3)

    # Take the first plausible Cloudflare Stream m3u8
    new_url = None
    for u in manifests:
        if "cloudflarestream.com" in u and u.endswith(".m3u8") or ".m3u8" in u:
            new_url = u
            break

    if not new_url:
        print("[error] found candidates but none looked like a proper m3u8.", file=sys.stderr)
        for u in manifests[:10]:
            print("  candidate:", u)
        sys.exit(4)

    # Optional: decode new exp just to log it
    new_exp = jwt_exp_from_url(new_url)
    if new_exp:
        dt_new = datetime.fromtimestamp(new_exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
        print(f"[info] new exp: {dt_new}  ({seconds_left_from_exp(new_exp)}s left)")

    # 3) Upsert back to Supabase
    if row.id is None:
        print("[error] row has no 'id' primary key; cannot update safely.", file=sys.stderr)
        sys.exit(5)

    update_stream_url(sb, args.table, row.id, new_url)
    print(f"[ok] Updated id={row.id} stream_url=web:{new_url}")

if __name__ == "__main__":
    main()
