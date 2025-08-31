# scripts/parsers/ontvtonight.py
from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as du
import pytz, re, os
from .base import Parser, Programme
from ..util.timeparse import normalize_window

TIME_ATTR_CANDIDATES = [
    "datetime", "data-datetime", "data-start", "data-starttime",
    "data-time", "data-begin"
]
TITLE_SELECTORS = [
    ".title", ".programme", ".program", ".show", ".program-title",
    ".listing-title", "h1", "h2", "h3", "h4", "strong", "a"
]

def _parse_any_datetime(s: str):
    if not s:
        return None
    s = s.strip()
    # epoch seconds/ms?
    if s.isdigit():
        try:
            n = int(s)
            if n > 10_000_000_000:
                n //= 1000
            return datetime.fromtimestamp(n, tz=pytz.UTC)
        except Exception:
            pass
    try:
        dt = du.parse(s)
        if dt.tzinfo is None:
            # leave naive (we'll localize later based on chosen align mode)
            return dt
        return dt
    except Exception:
        return None

def _align_dt_to_tz(dt_in, target_tz: str, mode: str):
    """
    mode='convert' -> normal timezone conversion (preserve instant).
    mode='shift'   -> keep the displayed clock time but reinterpret in target tz.

    Returns aware UTC datetime.
    """
    tz = pytz.timezone(target_tz)
    if isinstance(dt_in, datetime):
        # If aware
        if dt_in.tzinfo is not None:
            if mode == "convert":
                return dt_in.astimezone(pytz.UTC)
            else:  # shift
                # Preserve clock fields; rebuild in target tz.
                dt_local = tz.localize(datetime(dt_in.year, dt_in.month, dt_in.day, dt_in.hour, dt_in.minute, dt_in.second))
                return dt_local.astimezone(pytz.UTC)
        else:
            # Naive -> assume it's in target tz (guide already in MX)
            dt_local = tz.localize(dt_in)
            return dt_local.astimezone(pytz.UTC)
    return None

class OnTVTonightParser(Parser):
    """
    Parses On TV Tonight channel pages, e.g.:
      https://www.ontvtonight.com/guide/listings/channel/69048410/mvstv-mexico.html

    Strategy:
      - Fetch static HTML with httpx.
      - For each schedule row, find a <time datetime> or time-like data-* attribute
        and a nearby title node.
      - Align times to LOCAL_TZ using ONTV_ALIGN_MODE:
          * convert (default) or shift
      - End time = next row’s start; last = +60 minutes.
    """
    domains = ["ontvtonight.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        import httpx

        align_mode = os.getenv("ONTV_ALIGN_MODE", "convert").lower().strip()
        if align_mode not in ("convert", "shift"):
            align_mode = "convert"

        r = httpx.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Rows: grab many possible containers
        rows = soup.select("li, article, section, div.row, div.gridItem, tr, .listing, .program, .programme")
        items = []

        for row in rows:
            # 1) START TIME
            start_dt = None

            # Prefer <time datetime="...">
            t = row.find("time")
            if t and t.has_attr("datetime"):
                start_dt = _parse_any_datetime(t["datetime"])

            # Fallback: any time-like data-* on row or its descendants
            if start_dt is None:
                for attr in TIME_ATTR_CANDIDATES:
                    if row.has_attr(attr):
                        start_dt = _parse_any_datetime(row.get(attr))
                        if start_dt:
                            break
                if start_dt is None:
                    for el in row.find_all(True):
                        for attr in TIME_ATTR_CANDIDATES:
                            if el.has_attr(attr):
                                start_dt = _parse_any_datetime(el.get(attr))
                                if start_dt:
                                    break
                        if start_dt:
                            break

            if start_dt is None:
                continue

            # 2) TITLE
            title = ""
            for sel in TITLE_SELECTORS:
                el = row.select_one(sel)
                if el:
                    txt = el.get_text(" ", strip=True)
                    if txt:
                        title = txt
                        break
            if not title:
                # fallback: longest text fragment in the row
                title = max((s.strip() for s in row.stripped_strings), key=len, default="")
            if not title:
                continue

            # 3) ALIGN & TO UTC
            start_utc = _align_dt_to_tz(start_dt, tzname, align_mode)
            if not start_utc:
                continue

            items.append({"start_utc": start_utc, "title": title})

        # Sort and compute end times = next start; last +60m
        items.sort(key=lambda x: x["start_utc"])
        programmes = []
        for i, it in enumerate(items):
            s = it["start_utc"]
            e = items[i + 1]["start_utc"] if i + 1 < len(items) else s + timedelta(minutes=60)
            programmes.append(Programme(title=it["title"], start=s, end=e))

        return normalize_window(programmes, hours_ahead)
