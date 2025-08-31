# scripts/parsers/mvstv.py
from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import pytz, re
from .base import Parser, Programme
from ..util.timeparse import parse_spanish_time, normalize_window

DATE_RX = re.compile(
    r"^(Lunes|Martes|Mi[eí]rcoles|Jueves|Viernes|S[aá]bado|Domingo),?\s+(\d{1,2})\s+de\s+([A-Za-z\.]+)$",
    re.I,
)
TIME_RX = re.compile(r"\b(\d{1,2}:\d{2})(?:\s*(AM|PM))?\b", re.I)
ONLY_MERIDIEM_RX = re.compile(r"^(AM|PM|A\.M\.|P\.M\.)$", re.I)
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
    local_tz = pytz.timezone(tzname)
    today = datetime.now(local_tz).date()
    cand = date(today.year, mon, day)
    if (today - cand).days > 60:
        cand = date(today.year + 1, mon, day)
    return cand

def _looks_like_title(s: str) -> bool:
    s = s.strip()
    if not s:
        return False
    if ONLY_MERIDIEM_RX.match(s):  # "AM" / "PM"
        return False
    if DATE_RX.match(s):
        return False
    # must contain at least one letter (allow accents) and not be just a time
    if not re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", s):
        return False
    if TIME_RX.search(s):
        # if it's mostly a time string, skip; titles rarely embed a standalone time
        return False
    return True

class MVSTVParser(Parser):
    domains = ["mvstv.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        if not page:
            return []

        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)
        html = await page.content()

        soup = BeautifulSoup(html, "lxml")
        raw_lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines()]
        # filter obvious noise
        raw_lines = [ln for ln in raw_lines if ln and not SKIP_RX.search(ln)]

        # Merge time lines split across ["9:30", "AM"] into ["9:30 AM"]
        merged = []
        i = 0
        while i < len(raw_lines):
            cur = raw_lines[i]
            m = TIME_RX.search(cur)
            if m and not m.group(2):  # time present but no AM/PM attached
                if i + 1 < len(raw_lines) and ONLY_MERIDIEM_RX.match(raw_lines[i + 1]):
                    cur = f"{cur} {raw_lines[i + 1]}"
                    i += 1
            merged.append(cur)
            i += 1

        items = []  # {start_utc, date, title}
        i = 0
        while i < len(merged):
            ln = merged[i]
            tm = TIME_RX.search(ln)
            if not tm:
                i += 1
                continue

            # find following date line then title
            date_local = None
            title_txt = None
            j = i + 1

            while j < len(merged):
                if DATE_RX.match(merged[j]):
                    date_local = _parse_es_date(merged[j], tzname)
                    j += 1
                    break
                if TIME_RX.search(merged[j]):  # next card starts
                    break
                j += 1

            while date_local and j < len(merged):
                if TIME_RX.search(merged[j]):  # next card starts
                    break
                if _looks_like_title(merged[j]):
                    title_txt = merged[j]
                    j += 1
                    break
                j += 1

            if date_local and title_txt:
                local_midnight = datetime(date_local.year, date_local.month, date_local.day, 0, 0, 0)
                start_utc = parse_spanish_time(tm.group(0), local_midnight, tzname)
                items.append({"date": date_local, "title": title_txt, "start_utc": start_utc})
                i = j
            else:
                i += 1

        # End = next start on same local date, else +60m
        programmes = []
        for idx, it in enumerate(items):
            start_utc = it["start_utc"]
            end_utc = None
            for k in range(idx + 1, len(items)):
                if items[k]["date"] == it["date"]:
                    end_utc = items[k]["start_utc"]
                    break
            if end_utc is None:
                end_utc = start_utc + timedelta(minutes=60)
            programmes.append(Programme(title=it["title"], start=start_utc, end=end_utc))

        return normalize_window(programmes, hours_ahead)
