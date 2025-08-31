# scripts/parsers/mvstv.py
from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import pytz, re
from .base import Parser, Programme
from ..util.timeparse import parse_spanish_time, normalize_window

DAY_RX = re.compile(r"^(Lunes|Martes|Mi[eí]rcoles|Jueves|Viernes|S[aá]bado|Domingo)", re.I)
DATE_RX = re.compile(
    r"^(Lunes|Martes|Mi[eí]rcoles|Jueves|Viernes|S[aá]bado|Domingo),?\s+(\d{1,2})\s+de\s+([A-Za-z\.]+)$",
    re.I,
)
TIME_RX = re.compile(r"\b(\d{1,2}:\d{2})\s*(AM|PM)?\b", re.I)
SKIP_RX = re.compile(r"(EN VIVO|DESCARGA LA APP|PROGRAMACI[ÓO]N|DEPORTES|AGENDAR)", re.I)

MONTHS = {
    "ene": 1, "enero": 1,
    "feb": 2, "febrero": 2,
    "mar": 3, "marzo": 3,
    "abr": 4, "abril": 4,
    "may": 5, "mayo": 5,
    "jun": 6, "junio": 6,
    "jul": 7, "julio": 7,
    "ago": 8, "agosto": 8,
    "sep": 9, "sept": 9, "septiembre": 9,
    "oct": 10, "octubre": 10,
    "nov": 11, "noviembre": 11,
    "dic": 12, "diciembre": 12,
}

def _parse_es_date(line: str, tzname: str) -> date | None:
    m = DATE_RX.match(line.strip())
    if not m:
        return None
    _dow, day_str, mon_str = m.groups()
    day = int(day_str)
    mon = MONTHS.get(mon_str.lower().strip("."))
    if not mon:
        return None
    # choose the closest occurrence (this year or next) within ~2 months
    local_tz = pytz.timezone(tzname)
    today = datetime.now(local_tz).date()
    cand = date(today.year, mon, day)
    # if the candidate is > 60 days in the past, assume next year (site shows near-future schedule)
    if (today - cand).days > 60:
        cand = date(today.year + 1, mon, day)
    return cand

class MVSTVParser(Parser):
    domains = ["mvstv.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        """
        Renders https://mvstv.com/mvstv-programacion/ and extracts cards as
        (time -> date -> title) triples, e.g.:

           9:30 AM
           Domingo, 31 de Ago
           Doc Chat

        End time = next start (same day) or +60 min fallback.
        """
        if not page:
            return []  # we rely on Playwright for this JS-heavy site

        await page.goto(url, wait_until="domcontentloaded")
        # let lazy content settle
        await page.wait_for_timeout(1500)
        html = await page.content()

        soup = BeautifulSoup(html, "lxml")
        # Flatten visible text to lines
        raw_lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines()]
        # Filter obvious non-content noise
        lines = [ln for ln in raw_lines if ln and not SKIP_RX.search(ln)]

        items = []  # list of dicts: {"time": "...", "date": date, "title": "..."}
        i = 0
        while i < len(lines):
            ln = lines[i]
            tm = TIME_RX.search(ln)
            if not tm:
                i += 1
                continue

            # find the following date line and then the title line
            date_local = None
            title_txt = None
            j = i + 1

            # find date line
            while j < len(lines):
                if DATE_RX.match(lines[j]):
                    date_local = _parse_es_date(lines[j], tzname)
                    j += 1
                    break
                # If we encounter another time before a date, bail on this block
                if TIME_RX.search(lines[j]):
                    break
                j += 1

            # find title after date
            while date_local and j < len(lines):
                # stop if we hit the next card's time
                if TIME_RX.search(lines[j]):
                    break
                # first reasonable non-empty, non-noise line is the title
                if lines[j] and not SKIP_RX.search(lines[j]) and not DATE_RX.match(lines[j]):
                    title_txt = lines[j]
                    j += 1
                    break
                j += 1

            if date_local and title_txt:
                # Build start datetime in UTC using util's parse_spanish_time()
                local_midnight = datetime(
                    date_local.year, date_local.month, date_local.day, 0, 0, 0
                )
                start_utc = parse_spanish_time(tm.group(0), local_midnight, tzname)
                items.append(
                    {"time_str": tm.group(0), "date": date_local, "title": title_txt, "start_utc": start_utc}
                )
                i = j
            else:
                i += 1

        # Compute end times = next start on same day; else +60 minutes
        programmes = []
        for idx, it in enumerate(items):
            start_utc = it["start_utc"]
            # next item on same local date
            end_utc = None
            for k in range(idx + 1, len(items)):
                if items[k]["date"] == it["date"]:
                    end_utc = items[k]["start_utc"]
                    break
            if end_utc is None:
                end_utc = start_utc + timedelta(minutes=60)
            programmes.append(Programme(title=it["title"], start=start_utc, end=end_utc))

        return normalize_window(programmes, hours_ahead)
