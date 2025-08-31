# scripts/parsers/ontvtonight.py
from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dateutil import parser as du
import pytz, re, os
from .base import Parser, Programme
from ..util.timeparse import normalize_window

TIME_ATTR_CANDIDATES = ["datetime", "data-datetime", "data-start", "data-starttime", "data-time", "data-begin"]
TITLE_SELECTORS = [
    ".title", ".programme", ".program", ".show", ".program-title", ".listing-title",
    "h1", "h2", "h3", "h4", "strong", "a"
]

def _to_utc_from_any(s: str, tzname: str):
    if not s:
        return None
    s = s.strip()
    # Epoch seconds / ms
    if s.isdigit():
        try:
            n = int(s)
            if n > 10_000_000_000:  # ms
                n //= 1000
            dt = datetime.fromtimestamp(n, tz=pytz.timezone(tzname))
            return dt.astimezone(pytz.UTC)
        except Exception:
            pass
    # ISO / RFC / ical-ish strings
    try:
        dt = du.parse(s)
        if dt.tzinfo is None:
            dt = pytz.timezone(tzname).localize(dt)
        return dt.astimezone(pytz.UTC)
    except Exception:
        return None

class OnTVTonightParser(Parser):
    """
    Parses https://www.ontvtonight.com/guide/listings/channel/<id>/<slug>.html

    Strategy:
      - Fetch static HTML with httpx.
      - For each schedule row, look for a <time datetime="..."> or data-* attribute that carries
        the start time; use the nearest title element in that row.
      - End time = next row’s start; last row gets +60 minutes.
    """
    domains = ["ontvtonight.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        import httpx
        debug = os.getenv("DEBUG_ONTV") == "1"

        r = httpx.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Rows: be generous — many list/grid/table variants
        rows = soup.select("section, article, li, div.row, div.gridItem, tr")
        items = []

        for row in rows:
            # 1) start time (prefer <time datetime> or any time-like data-attr)
            start_utc = None
            t = row.find("time")
            if t and t.has_attr("datetime"):
                start_utc = _to_utc_from_any(t["datetime"], tzname)

            if not start_utc:
                # scan attributes on row and descendants
                # (common for aggregators to stash start in data-* at row level)
                for attr in TIME_ATTR_CANDIDATES:
                    if row.has_attr(attr):
                        start_utc = _to_utc_from_any(row.get(attr), tzname)
                        break
                if not start_utc:
                    for el in row.find_all(True):
                        for attr in TIME_ATTR_CANDIDATES:
                            if el.has_attr(attr):
                                start_utc = _to_utc_from_any(el.get(attr), tzname)
                                if start_utc:
                                    break
                        if start_utc:
                            break

            if not start_utc:
                continue  # can’t place the row without a start time

            # 2) title — pick the first reasonable text inside the same row
            title = ""
            for sel in TITLE_SELECTORS:
                el = row.select_one(sel)
                if el:
                    txt = el.get_text(" ", strip=True)
                    if txt:
                        title = txt
                        break
            if not title:
                # fallback: the longest non-empty text fragment in the row
                cand = max((s.strip() for s in row.stripped_strings), key=len, default="")
                title = cand

            if not title:
                continue

            items.append({"start_utc": start_utc, "title": title})

        # Sort and compute end times (next start; else +60)
        items.sort(key=lambda x: x["start_utc"])
        programmes = []
        for i, it in enumerate(items):
            s = it["start_utc"]
            e = items[i + 1]["start_utc"] if i + 1 < len(items) else s + timedelta(minutes=60)
            programmes.append(Programme(title=it["title"], start=s, end=e))

        if debug:
            print(f"[ontv] parsed {len(programmes)} items from page")

        return normalize_window(programmes, hours_ahead)
