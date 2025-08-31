# scripts/parsers/ontvtonight.py
from __future__ import annotations

import os
import re
from typing import List
from datetime import datetime, timedelta

import httpx
import pytz
from bs4 import BeautifulSoup
from dateutil import parser as du

from .base import Parser, Programme
from ..util.timeparse import normalize_window

# ---------- Config / heuristics ----------
TIME_ATTR_CANDIDATES = [
    "datetime", "data-datetime", "data-start", "data-starttime",
    "data-time", "data-begin"
]
TITLE_SELECTORS = [
    ".program-title", ".programme-title", ".listing-title", ".title",
    ".program", ".programme", "h1", "h2", "h3", "h4", "a", "strong"
]
BAD_TITLE_RX = re.compile(r"^(AM|PM|A\.M\.|P\.M\.|Close|Cerrar|Programación pagada)$", re.I)

TIME_LINE_RX = re.compile(r'^\s*(\d{1,2}):(\d{2})\s*(am|pm)\s*$', re.I)
DATE_LINE_RX = re.compile(r'^\d{4}-\d{2}-\d{2}$')  # e.g., 2025-08-30 on the page

# ---------- Time helpers ----------
def _parse_any_dt(s: str):
    if not s:
        return None
    s = s.strip()
    if s.isdigit():
        try:
            n = int(s)
            if n > 10_000_000_000:
                n //= 1000
            return datetime.fromtimestamp(n, tz=pytz.UTC)
        except Exception:
            pass
    try:
        return du.parse(s)
    except Exception:
        return None

def _align_to_utc(local_dt: datetime, tzname: str, mode: str):
    """mode='convert' preserves instant; mode='shift' preserves clock time in target tz."""
    tz = pytz.timezone(tzname)
    if local_dt.tzinfo is None:
        local_dt = tz.localize(local_dt)
    if mode == "convert":
        return local_dt.astimezone(pytz.UTC)
    # shift: reinterpret the naive clock time in target tz
    shifted = tz.localize(datetime(local_dt.year, local_dt.month, local_dt.day,
                                   local_dt.hour, local_dt.minute, getattr(local_dt, "second", 0)))
    return shifted.astimezone(pytz.UTC)

# ---------- Structured DOM extraction ----------
def _extract_structured(soup: BeautifulSoup, tzname: str, align_mode: str) -> List[Programme]:
    programmes: List[Programme] = []

    # Find every <time> that carries a starting timestamp; anchor the row by climbing a bit
    for t in soup.find_all("time"):
        raw = None
        # Prefer <time datetime="...">
        if t.has_attr("datetime"):
            raw = t.get("datetime")
        # Or any time-like attribute on the tag
        if raw is None:
            for attr in TIME_ATTR_CANDIDATES:
                if t.has_attr(attr):
                    raw = t.get(attr)
                    break
        if not raw:
            continue

        dt = _parse_any_dt(raw)
        if not dt:
            continue

        # Align to UTC
        start_utc = _align_to_utc(dt, tzname, align_mode) if dt.tzinfo else _align_to_utc(dt, tzname, "convert")

        # Row container: go up to a reasonable block (<li>, <tr>, or div with classes)
        row = t
        for _ in range(6):
            if not row.parent:
                break
            row = row.parent
            # Stop when this block contains a title-looking element
            if any(row.select(sel) for sel in TITLE_SELECTORS):
                break

        # Title within this row
        title = ""
        for sel in TITLE_SELECTORS:
            el = row.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt and not BAD_TITLE_RX.match(txt):
                    title = txt
                    break
        if not title:
            # fallback: longest non-empty string in row, excluding bad tokens
            best = ""
            for frag in row.stripped_strings:
                frag = frag.strip()
                if frag and not BAD_TITLE_RX.match(frag):
                    if len(frag) > len(best):
                        best = frag
            title = best

        if not title:
            continue

        programmes.append(Programme(title=title, start=start_utc, end=start_utc))  # temp end

    # Sort & compute end from next start (+60 min fallback)
    programmes.sort(key=lambda p: p.start)
    result: List[Programme] = []
    for i, p in enumerate(programmes):
        s = p.start
        e = programmes[i + 1].start if i + 1 < len(programmes) else s + timedelta(minutes=60)
        result.append(Programme(title=p.title, start=s, end=e))
    return result

# ---------- Plain-text fallback (your snippet) ----------
def _fallback_parse_text(soup: BeautifulSoup, tzname: str, hours_ahead: int, align_mode: str):
    """
    Text-mode fallback for pages that render schedule as plain text lines:
      time line (e.g., '10:30 am') -> title line (e.g., 'Doc Chat')
    Occasional lines like '2025-08-30' reset the current date bucket.
    """
    lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines()]
    lines = [ln for ln in lines if ln]  # drop empties

    # Track current local date seen on the page; default to 'today' in MX
    mx_today = datetime.now(pytz.timezone(tzname)).date()
    current_date = mx_today

    items = []  # will collect (start_utc, title)

    i = 0
    while i < len(lines):
        ln = lines[i]

        # date marker like 2025-08-30
        if DATE_LINE_RX.match(ln):
            try:
                current_date = du.parse(ln).date()
            except Exception:
                pass
            i += 1
            continue

        m = TIME_LINE_RX.match(ln)
        if not m:
            i += 1
            continue

        # next non-heading, non-bad line is the title
        j = i + 1
        title = None
        while j < len(lines):
            cand = lines[j]
            # skip headings that appear on the page
            if cand.lower() in ("time", "tv show", "hora", "programa"):
                j += 1
                continue
            if BAD_TITLE_RX.match(cand):
                j += 1
                continue
            title = cand
            break

        if title:
            hh, mm, mer = int(m.group(1)), int(m.group(2)), m.group(3).lower()
            if mer == "pm" and hh != 12:
                hh += 12
            if mer == "am" and hh == 12:
                hh = 0
            local_dt = datetime(current_date.year, current_date.month, current_date.day, hh, mm)
            start_utc = _align_to_utc(local_dt, tzname, align_mode)
            items.append((start_utc, title))
            i = j + 1
        else:
            i += 1

    # compute end = next start; last +60m
    items.sort(key=lambda x: x[0])
    programmes = []
    for idx, (s_utc, title) in enumerate(items):
        e_utc = items[idx + 1][0] if idx + 1 < len(items) else s_utc + timedelta(minutes=60)
        programmes.append(Programme(title=title, start=s_utc, end=e_utc))

    return programmes

# ---------- Parser class ----------
class OnTVTonightParser(Parser):
    domains = ["ontvtonight.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None) -> List[Programme]:
        align_mode = os.getenv("ONTV_ALIGN_MODE", "convert").lower().strip()
        if align_mode not in ("convert", "shift"):
            align_mode = "convert"

        # Fetch static HTML
        r = httpx.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # 1) Structured extraction
        progs = _extract_structured(soup, tzname, align_mode)

        # 2) Fallback to plain-text if nothing structured found
        if not progs:
            progs = _fallback_parse_text(soup, tzname, hours_ahead, align_mode)

        # De-dupe within page (title + start)
        seen = set()
        deduped: List[Programme] = []
        for p in progs:
            key = (p.title, p.start)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(p)

        return normalize_window(deduped, hours_ahead)
