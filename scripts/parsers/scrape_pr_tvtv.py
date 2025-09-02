#!/usr/bin/env python3
"""
Scrape TV schedules from tvtv.us Puerto Rico pages using Playwright.

Why Playwright? tvtv.us is client-rendered (Next.js). We extract the
`__NEXT_DATA__` JSON (or fall back to sniffing JSON XHRs) and parse out
program items for one or more station pages.

Usage (module):
  python -m scripts.scrape_pr_tvtv \
    --city bayamon \
    --lineup luUSA-PR68592-X \
    --station 43726 \
    --hours-ahead 36 \
    --program "PR\\s*en\\s*Vivo" \
    --local-tz America/Puerto_Rico \
    --output out/pr_en_vivo.jsonl

Or via env (good for GitHub Actions):
  CITY_SLUG=bayamon LINEUP_ID=luUSA-PR68592-X STATION_IDS=43726 \
  HOURS_AHEAD=36 LOCAL_TZ=America/Puerto_Rico \
  python -m scripts.scrape_pr_tvtv

Notes:
- We keep output simple: newline-delimited JSON with normalized fields.
- If PROGRAM_REGEX is set (CLI or env), results are filtered to matches.
- Times are exported as both UTC and local tz strings.
- Caching/sign-in is NOT required.

Requires: playwright (Chromium). In CI, run `python -m playwright install --with-deps chromium`.
"""
from __future__ import annotations

import argparse
import asyncio
import dataclasses as dc
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from playwright.async_api import async_playwright, Browser, Page

try:
    # Py3.9+
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# -------------------------------
# Data structures
# -------------------------------

@dc.dataclass(frozen=True)
class Program:
    station_id: str
    station_name: Optional[str]
    title: str
    subtitle: Optional[str]
    description: Optional[str]
    start_utc: str  # ISO8601 UTC
    end_utc: Optional[str]  # ISO8601 UTC
    start_local: str  # ISO8601 with local tz offset
    end_local: Optional[str]
    season: Optional[int] = None
    episode: Optional[int] = None


# -------------------------------
# Helpers
# -------------------------------

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape tvtv.us PR station schedules")
    p.add_argument("--base-url", default=_env("BASE_URL", "https://www.tvtv.us/pr"))
    p.add_argument("--city", default=_env("CITY_SLUG", "bayamon"), help="City slug (e.g., bayamon)")
    p.add_argument("--lineup", default=_env("LINEUP_ID", "luUSA-PR68592-X"), help="Lineup id segment")
    p.add_argument(
        "--station", action="append", dest="stations", default=None,
        help="Station id (repeatable). If omitted, reads STATION_IDS=comma,separated",
    )
    p.add_argument("--hours-ahead", type=int, default=int(_env("HOURS_AHEAD", "36")), help="Horizon hours")
    p.add_argument("--local-tz", default=_env("LOCAL_TZ", "America/Puerto_Rico"))
    p.add_argument("--program", default=_env("PROGRAM_REGEX"), help="Regex filter for program title (optional)")
    p.add_argument("--output", default=_env("OUTPUT_JSONL", None), help="Write NDJSON to this path (optional)")
    p.add_argument("--headless", default=_env("HEADLESS", "true"))
    p.add_argument("--timeout-ms", type=int, default=int(_env("NAV_TIMEOUT_MS", "30000")))
    return p.parse_args(argv)


def _parse_station_list(args: argparse.Namespace) -> List[str]:
    if args.stations:
        return args.stations
    env_ids = _env("STATION_IDS")
    if env_ids:
        return [s.strip() for s in env_ids.split(",") if s.strip()]
    # Fallback to TeleMundo PR example from the convo (can be overridden)
    return ["43726"]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_tz(name: str) -> ZoneInfo:
    if ZoneInfo is None:
        raise RuntimeError("zoneinfo not available; use Python 3.9+ or install backports.zoneinfo")
    return ZoneInfo(name)


def _to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z") if dt.tzinfo == timezone.utc else dt.isoformat()


def _parse_time_loosely(val: Any) -> Optional[datetime]:
    """Parse a variety of time formats to aware UTC datetime when possible.
    Supports:
    - epoch seconds/millis
    - ISO strings with/without Z (assumed UTC if Z / offset provided)
    - fallback: None
    """
    if val is None:
        return None
    # numbers
    if isinstance(val, (int, float)):
        x = float(val)
        if x > 1e12:  # ms
            return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
        if x > 1e10:  # sec (far future)
            return datetime.fromtimestamp(x, tz=timezone.utc)
        # too small to be epoch; ignore
        return None
    # strings
    if isinstance(val, str):
        s = val.strip()
        # ISO-ish
        try:
            if s.endswith("Z"):
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            if "+" in s[10:]:  # contains offset
                return datetime.fromisoformat(s)
            # bare ISO assume local? we can't know; treat as naive UTC
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _rec_find_program_nodes(obj: Any) -> Iterable[Dict[str, Any]]:
    """Yield dicts that look like program items (have title and start/time-ish keys)."""
    def looks_like_program(d: Dict[str, Any]) -> bool:
        if not isinstance(d, dict):
            return False
        keys = set(k.lower() for k in d.keys())
        if "title" not in keys:
            return False
        time_keys = {"start", "end", "starttime", "endtime", "startdatetime", "enddatetime"}
        return len(keys & time_keys) >= 1

    if isinstance(obj, dict):
        if looks_like_program(obj):
            yield obj
        for v in obj.values():
            yield from _rec_find_program_nodes(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _rec_find_program_nodes(it)


def _rec_find_station_name(obj: Any) -> Optional[str]:
    # Try common fields
    if isinstance(obj, dict):
        name = obj.get("stationName") or obj.get("callSign") or obj.get("station") or obj.get("channelName")
        if isinstance(name, str) and len(name) >= 2:
            return name
        for v in obj.values():
            n = _rec_find_station_name(v)
            if n:
                return n
    if isinstance(obj, list):
        for it in obj:
            n = _rec_find_station_name(it)
            if n:
                return n
    return None


async def _extract_next_data_json(page: Page) -> Optional[Dict[str, Any]]:
    # Primary: __NEXT_DATA__ script tag
    try:
        el = await page.wait_for_selector("script#__NEXT_DATA__", timeout=5000)
        txt = await el.text_content()
        if txt:
            return json.loads(txt)
    except Exception:
        pass

    # Fallback: sniff JSON XHR responses that look rich
    captured: List[Dict[str, Any]] = []

    def on_response(resp):
        try:
            ct = resp.headers.get("content-type", "")
            if "application/json" in ct:
                # schedule requests tend to be larger
                if resp.request and any(k in resp.url for k in ["schedule", "listing", "grid", "stn", "lineup", "_next", "data"]):
                    asyncio.create_task(_collect_json(resp, captured))
        except Exception:
            pass

    page.on("response", on_response)
    # give it a chance
    await page.wait_for_timeout(2000)
    page.off("response", on_response)
    # Pick the biggest JSON blob
    if captured:
        captured.sort(key=lambda d: len(json.dumps(d, ensure_ascii=False)), reverse=True)
        return captured[0]
    return None


async def _collect_json(resp, bucket: List[Dict[str, Any]]):
    try:
        data = await resp.json()
        if isinstance(data, dict) and data:
            bucket.append(data)
    except Exception:
        pass


async def fetch_station_programs(base_url: str, city: str, lineup: str, station_id: str, hours_ahead: int, local_tz: str, headless: bool, timeout_ms: int) -> Tuple[str, List[Program]]:
    url = f"{base_url.rstrip('/')}/{city}/{lineup}/stn/{station_id}"
    print(f"[INFO] Fetching station {station_id} → {url}")

    tz = _ensure_tz(local_tz)
    horizon_utc = _now_utc() + timedelta(hours=hours_ahead)

    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(headless=(str(headless).lower() != "false"))
        page: Page = await browser.new_page()
        page.set_default_timeout(timeout_ms)
        await page.goto(url, wait_until="domcontentloaded")
        # Let the client do its data pulls
        await page.wait_for_load_state("networkidle")
        data = await _extract_next_data_json(page)
        await browser.close()

    if not data:
        print("[WARN] Could not extract JSON from page; no results.")
        return station_id, []

    station_name = _rec_find_station_name(data)
    programs: List[Program] = []

    for node in _rec_find_program_nodes(data):
        title = str(node.get("title") or node.get("programTitle") or "").strip()
        if not title:
            continue
        subtitle = node.get("subtitle") or node.get("episodeTitle")
        desc = node.get("description") or node.get("desc") or node.get("synopsis")
        s_num = node.get("season") or node.get("seasonNumber")
        e_num = node.get("episode") or node.get("episodeNumber")
        start_raw = node.get("start") or node.get("startTime") or node.get("startDateTime")
        end_raw = node.get("end") or node.get("endTime") or node.get("endDateTime")

        start_dt_utc = _parse_time_loosely(start_raw)
        end_dt_utc = _parse_time_loosely(end_raw)

        if not start_dt_utc:
            # Some datasets encode minutes-since-midnight + date; try other hints
            continue

        # Filter by horizon
        if start_dt_utc.tzinfo is None:
            start_dt_utc = start_dt_utc.replace(tzinfo=timezone.utc)
        if end_dt_utc and end_dt_utc.tzinfo is None:
            end_dt_utc = end_dt_utc.replace(tzinfo=timezone.utc)

        if start_dt_utc > horizon_utc:
            continue

        start_local = start_dt_utc.astimezone(tz)
        end_local = end_dt_utc.astimezone(tz) if end_dt_utc else None

        programs.append(
            Program(
                station_id=station_id,
                station_name=station_name,
                title=title,
                subtitle=str(subtitle).strip() if subtitle else None,
                description=str(desc).strip() if desc else None,
                start_utc=_to_iso(start_dt_utc),
                end_utc=_to_iso(end_dt_utc) if end_dt_utc else None,
                start_local=_to_iso(start_local),
                end_local=_to_iso(end_local) if end_local else None,
                season=int(s_num) if isinstance(s_num, (int, float, str)) and str(s_num).isdigit() else None,
                episode=int(e_num) if isinstance(e_num, (int, float, str)) and str(e_num).isdigit() else None,
            )
        )

    # Deduplicate by (title, start_utc)
    seen = set()
    uniq: List[Program] = []
    for p in programs:
        key = (p.title, p.start_utc)
        if key not in seen:
            seen.add(key)
            uniq.append(p)

    return station_id, uniq


def _compile_regex(maybe_pat: Optional[str]) -> Optional[re.Pattern[str]]:
    if not maybe_pat:
        return None
    try:
        return re.compile(maybe_pat, re.IGNORECASE)
    except re.error:
        print(f"[WARN] Invalid regex pattern; ignoring: {maybe_pat}")
        return None


async def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    stations = _parse_station_list(args)
    prog_re = _compile_regex(args.program)

    out_path = Path(args.output) if args.output else None
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)

    all_rows: List[Program] = []

    for stn in stations:
        try:
            _, rows = await fetch_station_programs(
                base_url=args.base_url,
                city=args.city,
                lineup=args.lineup,
                station_id=stn,
                hours_ahead=args.hours_ahead,
                local_tz=args.local_tz,
                headless=args.headless,
                timeout_ms=args.timeout_ms,
            )
        except Exception as e:
            print(f"[ERROR] Station {stn}: {e}")
            continue

        # Optional filter by title regex
        if prog_re:
            rows = [r for r in rows if prog_re.search(r.title or "")]
        all_rows.extend(rows)

    # Sort by start time
    all_rows.sort(key=lambda r: r.start_utc)

    # Human summary
    print("\n[SUMMARY]")
    if not all_rows:
        print("No programs found for the given criteria.")
    else:
        for r in all_rows[:20]:  # cap spam
            name = r.station_name or r.station_id
            print(f"{r.start_local} — {r.title} ({name})")

    # Emit NDJSON
    def to_dict(p: Program) -> Dict[str, Any]:
        return {
            "station_id": p.station_id,
            "station_name": p.station_name,
            "title": p.title,
            "subtitle": p.subtitle,
            "description": p.description,
            "start_utc": p.start_utc,
            "end_utc": p.end_utc,
            "start_local": p.start_local,
            "end_local": p.end_local,
            "season": p.season,
            "episode": p.episode,
            "source": "tvtv.us",
        }

    if out_path:
        with out_path.open("w", encoding="utf-8") as f:
            for r in all_rows:
                f.write(json.dumps(to_dict(r), ensure_ascii=False) + "\n")
        print(f"[OK] Wrote {len(all_rows)} rows → {out_path}")
    else:
        for r in all_rows:
            print(json.dumps(to_dict(r), ensure_ascii=False))

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        raise SystemExit(130)
