# add near the top with your other imports
import re
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from dateutil import parser as du
from .base import Programme
from ..util.timeparse import normalize_window

TIME_LINE_RX = re.compile(r'^\s*(\d{1,2}):(\d{2})\s*(am|pm)\s*$', re.I)
DATE_LINE_RX = re.compile(r'^\d{4}-\d{2}-\d{2}$')  # e.g., 2025-08-30 on the page
BAD_TITLE_RX = re.compile(r'^(AM|PM|A\.M\.|P\.M\.|Close|Cerrar|Programación pagada)$', re.I)

def _align_to_utc(local_dt: datetime, tzname: str, mode: str):
    """mode='convert' preserves instant, mode='shift' preserves clock time in target tz."""
    tz = pytz.timezone(tzname)
    if local_dt.tzinfo is None:
        local_dt = tz.localize(local_dt)
    if mode == "convert":
        return local_dt.astimezone(pytz.UTC)
    # shift: reinterpret the naive clock time in target tz
    shifted = tz.localize(datetime(local_dt.year, local_dt.month, local_dt.day,
                                   local_dt.hour, local_dt.minute, getattr(local_dt, "second", 0)))
    return shifted.astimezone(pytz.UTC)

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

        # merge with the next non-heading, non-bad title line
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

            # jump to j for next iteration
            i = j + 1
        else:
            i += 1

    # compute end = next start; last +60m
    items.sort(key=lambda x: x[0])
    programmes = []
    for idx, (s_utc, title) in enumerate(items):
        e_utc = items[idx + 1][0] if idx + 1 < len(items) else s_utc + timedelta(minutes=60)
        programmes.append(Programme(title=title, start=s_utc, end=e_utc))

    return normalize_window(programmes, hours_ahead)
