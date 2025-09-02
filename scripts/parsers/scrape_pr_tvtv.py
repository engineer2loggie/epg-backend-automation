#!/usr/bin/env python3
"""
Scrape TV schedules from tvtv.us Puerto Rico pages using Playwright.

- Navigates EXACTLY to: https://www.tvtv.us/pr/<city>/<lineup>/stn/<stationId>
- Extracts Next.js __NEXT_DATA__ or falls back to JSON XHRs
- Normalizes times to UTC and a provided LOCAL_TZ
- Optional PROGRAM_REGEX filter
- Debug mode can dump the captured JSON for inspection

Example:
  python -m scripts.parsers.scrape_pr_tvtv \
    --city bayamon \
    --lineup luUSA-PR68592-X \
    --station 43726 \
    --hours-ahead 36 \
    --local-tz America/Puerto_Rico \
    --output out/pr_tvtv.jsonl --debug
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

from playwright.async_api import async_playwright, Browser, Page, BrowserContext

try:
    from zoneinfo import ZoneInfo  # py3.9+
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
    start_utc: str
    end_utc: Optional[str]
    start_local: str
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
    p.add_argument("--station", action="append", dest="stations", default=None, help="Station id (repeatable)")
    p.add_argument("--hours-ahead", type=int, default=int(_env("HOURS_AHEAD", "36")), help="Horizon hours")
    p.add_argument("--local-tz", default=_env("LOCAL_TZ", "America/Puerto_Rico"))
    p.add_argument("--program", default=_env("PROGRAM_REGEX"), help="Regex filter for program title (optional)")
    p.add_argument("--output", default=_env("OUTPUT_JSONL", None), help="Write NDJSON to this path (optional)")
    p.add_argument("--headless", default=_env("HEADLESS", "true"))
    p.add_argument("--timeout-ms", type=int, default=int(_env("NAV_TIMEOUT_MS", "35000")))
    p.add_argument("--debug", action="store_true", default=bool(int(_env("DEBUG_TVTV", "0"))), help="Dump captured JSON")
    return p.parse_args(argv)


def _parse_station_list(args: argparse.Namespace) -> List[str]:
    if args.stations:
        return args.stations
    env_ids = _env("STATION_IDS")
    if env_ids:
        return [s.strip() for s in env_ids.split(",") if s.strip()]
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
    """Parse many time formats into a datetime. Returns UTC-aware if possible.
    Supports: epoch sec/ms, ISO (with/without Z), "H:MM" and "H:MM AM/PM" (naive),
    and minutes-from-midnight (<= 1440).
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        x = float(val)
        # minutes-from-midnight heuristic
        if 0 <= x <= 24 * 60:
            return datetime(1970, 1, 1, 0, 0) + timedelta(minutes=int(x))  # naive
        if x > 1e12:  # ms
            return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
        if x > 1e10:  # sec (far future)
            return datetime.fromtimestamp(x, tz=timezone.utc)
        return None
    if isinstance(val, str):
        s = val.strip()
        # time-only like "1:00 PM" or "13:00"
        m = re.fullmatch(r"^([0-9]{1,2}):([0-9]{2})(?:[ ]*([AP]M))?$", s, flags=re.IGNORECASE)
        if m:
            h = int(m.group(1))
            mm = int(m.group(2))
            ampm = m.group(3)
            if ampm:
                ampm = ampm.upper()
                if ampm == "PM" and h != 12:
                    h += 12
                if ampm == "AM" and h == 12:
                    h = 0
            return datetime(1970, 1, 1, h, mm)  # naive
        try:
            if s.endswith("Z"):
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            if "+" in s[10:]:  # contains offset
                return datetime.fromisoformat(s)
            # bare ISO treated as UTC
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _rec_find_program_nodes(obj: Any) -> Iterable[Dict[str, Any]]:
    """Yield dicts that look like program items."""
    def looks_like_program(d: Dict[str, Any]) -> bool:
        if not isinstance(d, dict):
            return False
        keys = {k.lower() for k in d.keys()}
        if not ("title" in keys or "programtitle" in keys or "name" in keys):
            return False
        time_keys = {
            "start", "end", "starttime", "endtime", "startdatetime", "enddatetime",
            "airingstarttime", "airingendtime", "start_minutes", "startminutes",
            "minutes", "startmin", "duration", "lengthminutes"
        }
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
    """Try multiple strategies to get rich JSON from a Next.js page.
    1) __NEXT_DATA__
    2) JSON XHRs captured during/after navigation (schedule/grid/etc)
    """
    # 1) __NEXT_DATA__
    try:
        el = await page.wait_for_selector("script#__NEXT_DATA__", timeout=12000)
        txt = await el.text_content()
        if txt:
            return json.loads(txt)
    except Exception:
        pass

    # 2) Capture JSON responses
    captured: List[Dict[str, Any]] = []

    async def on_response(resp):
        try:
            ct = resp.headers.get("content-type", "")
            if "application/json" in ct:
                if any(k in resp.url for k in ["schedule", "listing", "grid", "stn", "lineup", "_next", "data"]):
                    try:
                        data = await resp.json()
                        if isinstance(data, dict) and data:
                            captured.append(data)
                    except Exception:
                        pass
        except Exception:
            pass

    page.on("response", on_response)
    try:
        await page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(3000)
    page.off("response", on_response)

    if captured:
        captured.sort(key=lambda d: len(json.dumps(d, ensure_ascii=False)), reverse=True)
        return captured[0]
    return None


async def fetch_station_programs(base_url: str, city: str, lineup: str, station_id: str, hours_ahead: int, local_tz: str, headless: bool, timeout_ms: int, debug: bool = False, dump_dir: Optional[Path] = None) -> Tuple[str, List[Program]]:
    url = f"{base_url.rstrip('/')}/{city}/{lineup}/stn/{station_id}"
    print(f"[INFO] Fetching station {station_id} → {url}")

    tz = _ensure_tz(local_tz)
    horizon_utc = _now_utc() + timedelta(hours=hours_ahead)

    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(headless=(str(headless).lower() != "false"))
        ctx: BrowserContext = await browser.new_context(
            locale="en-US",
            timezone_id=local_tz,
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
            ),
        )
        page: Page = await ctx.new_page()
        page.set_default_timeout(timeout_ms)
        await page.goto(url, wait_until="domcontentloaded")
        data = await _extract_next_data_json(page)
        if debug and dump_dir and data:
            dump_dir.mkdir(parents=True, exist_ok=True)
            sample = dump_dir / f"tvtv_{station_id}.json"
            sample.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        await ctx.close()
        await browser.close()

    if not data:
        print("[WARN] Could not extract JSON from page; no results.")
        return station_id, []

    station_name = _rec_find_station_name(data)
    programs: List[Program] = []

    # Try to infer a context date (e.g., gridDate: YYYY-MM-DD) for time-only values
    payload_text = json.dumps(data, ensure_ascii=False)
    m_date = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", payload_text)
    context_date = m_date.group(1) if m_date else None

    def attach_date_if_time_only(dt_like: datetime) -> datetime:
        if dt_like.tzinfo is not None:
            return dt_like
        base = context_date or datetime.now(tz).date().isoformat()
        y, mo, d = map(int, base.split("-"))
        dt_local = datetime(y, mo, d, dt_like.hour, dt_like.minute, tzinfo=tz)
        return dt_local.astimezone(timezone.utc)

    for node in _rec_find_program_nodes(data):
        title = (str(node.get("title") or node.get("programTitle") or node.get("name") or "")).strip()
        if not title:
            prog = node.get("program") if isinstance(node, dict) else None
            if isinstance(prog, dict):
                title = (str(prog.get("title") or prog.get("name") or "")).strip()
        if not title:
            continue

        subtitle = node.get("subtitle") or node.get("episodeTitle")
        if not subtitle and isinstance(node.get("program"), dict):
            subtitle = node["program"].get("episodeTitle")

        desc = node.get("description") or node.get("desc") or node.get("synopsis")
        if not desc and isinstance(node.get("program"), dict):
            desc = node["program"].get("synopsis")

        s_num = node.get("season") or node.get("seasonNumber")
        e_num = node.get("episode") or node.get("episodeNumber")

        start_raw = (
            node.get("start") or node.get("startTime") or node.get("startDateTime") or
            node.get("airingStartTime") or node.get("start_minutes") or node.get("startMinutes") or
            node.get("startMin") or node.get("minutes")
        )
        end_raw = (
            node.get("end") or node.get("endTime") or node.get("endDateTime") or node.get("airingEndTime")
        )
        duration_min = node.get("duration") or node.get("lengthMinutes") or node.get("dur")

        start_dt_utc = _parse_time_loosely(start_raw)
        end_dt_utc = _parse_time_loosely(end_raw)

        # Handle naive times / minutes-from-midnight
        if start_dt_utc and start_dt_utc.tzinfo is None:
            start_dt_utc = attach_date_if_time_only(start_dt_utc)
        elif isinstance(start_raw, (int, float)) and float(start_raw) <= 24 * 60:
            minutes = int(float(start_raw))
            base = context_date or datetime.now(tz).date().isoformat()
            y, mo, d = map(int, base.split("-"))
            dt_local = datetime(y, mo, d, 0, 0, tzinfo=tz) + timedelta(minutes=minutes)
            start_dt_utc = dt_local.astimezone(timezone.utc)

        if not start_dt_utc:
            continue

        if end_dt_utc and end_dt_utc.tzinfo is None:
            end_dt_utc = attach_date_if_time_only(end_dt_utc)
        if not end_dt_utc and duration_min:
            try:
                end_dt_utc = start_dt_utc + timedelta(minutes=int(float(duration_min)))
            except Exception:
                end_dt_utc = None

        # Horizon filter
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

    dump_dir = Path("logs/_tvtv_debug") if args.debug else None

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
                debug=args.debug,
                dump_dir=dump_dir,
            )
        except Exception as e:
            print(f"[ERROR] Station {stn}: {e}")
            continue

        if prog_re:
            rows = [r for r in rows if prog_re.search(r.title or "")]
        all_rows.extend(rows)

    all_rows.sort(key=lambda r: r.start_utc)

    print("\n[SUMMARY]")
    if not all_rows:
        print("No programs found for the given criteria.")
    else:
        for r in all_rows[:30]:
            name = r.station_name or r.station_id
            print(f"{r.start_local} — {r.title} ({name})")

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
