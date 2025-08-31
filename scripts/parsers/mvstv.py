# scripts/parsers/mvstv.py
from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import pytz, re, os
from .base import Parser, Programme
from ..util.timeparse import parse_spanish_time, normalize_window

DATE_RX = re.compile(
    r"^(Lunes|Martes|Mi[eí]rcoles|Jueves|Viernes|S[aá]bado|Domingo),?\s+(\d{1,2})\s+de\s+([A-Za-z\.]+)$",
    re.I,
)
ONLY_MERIDIEM_RX = re.compile(r"^(AM|PM|A\.M\.|P\.M\.)$", re.I)

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
    s = (s or "").strip()
    if not s:
        return False
    if ONLY_MERIDIEM_RX.match(s):
        return False
    if DATE_RX.match(s):
        return False
    # must contain letters; avoid pure tokens
    return bool(re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", s))

class MVSTVParser(Parser):
    domains = ["mvstv.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        """
        DOM-scoped extraction per program card:
        - Find each card by locating the 'AGENDAR' button and walking to its container.
        - Inside that container, read:
            * time: first text node matching \d{1,2}:\d{2} with optional AM/PM
            * date: 'Domingo, 31 de Ago' style line
            * title: first heading (h1-h4/strong) or non-time, non-date text
        - End time = next start (same local date) else +60 min.
        """
        if not page:
            return []

        await page.goto(url, wait_until="domcontentloaded")
        # allow lazy content to mount
        await page.wait_for_timeout(1500)

        # Get HTML for evaluation (we still rely on Playwright's render)
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")

        # Each card has an 'AGENDAR' button — use it to anchor the card container
        agenda_nodes = soup.find_all(string=re.compile(r"\bAGENDAR\b", re.I))
        cards = []
        for node in agenda_nodes:
            el = getattr(node, "parent", None)
            container = None
            # Walk up to a reasonable card container
            for _ in range(8):
                if not el:
                    break
                # Heuristic: container should contain both a time and a date line
                block_text = el.get_text("\n", strip=True)
                if re.search(r"\b\d{1,2}:\d{2}\s*(AM|PM)?\b", block_text, re.I) and DATE_RX.search(block_text):
                    container = el
                    break
                el = getattr(el, "parent", None)
            if container:
                cards.append(container)

        # Fallback: if no cards found, parse nothing (avoid global text that caused PM/AM titles)
        if not cards:
            return []

        programmes = []
        debug = os.getenv("DEBUG_MVSTV") == "1"

        def extract_card(card_el):
            text_lines = [ln.strip() for ln in card_el.get_text("\n", strip=True).splitlines() if ln.strip()]
            # 1) TIME: merge split time tokens like ["9:30", "AM"] => "9:30 AM"
            time_txt = None
            i = 0
            while i < len(text_lines):
                t = text_lines[i]
                m = re.search(r"\b(\d{1,2}:\d{2})\b", t)
                if m:
                    # try to append meridiem from next line
                    mer = None
                    if i + 1 < len(text_lines) and ONLY_MERIDIEM_RX.match(text_lines[i + 1]):
                        mer = text_lines[i + 1]
                        i += 1
                    time_txt = m.group(1) + (f" {mer}" if mer else "")
                    break
                i += 1

            # 2) DATE
            date_txt = next((ln for ln in text_lines if DATE_RX.match(ln)), None)
            date_local = _parse_es_date(date_txt, tzname) if date_txt else None

            # 3) TITLE
            # Prefer heading-like elements
            title_txt = None
            for h in card_el.select("h1,h2,h3,h4,strong"):
                tt = h.get_text(" ", strip=True)
                if _looks_like_title(tt):
                    title_txt = tt
                    break
            if not title_txt:
                # fallback: first non-time, non-date, non-meridiem line with letters
                for ln in text_lines:
                    if DATE_RX.match(ln) or ONLY_MERIDIEM_RX.match(ln) or re.search(r"\b\d{1,2}:\d{2}\b", ln):
                        continue
                    if _looks_like_title(ln):
                        title_txt = ln
                        break

            return time_txt, date_local, title_txt

        items = []
        for card in cards:
            t_txt, d_local, title = extract_card(card)
            if not (t_txt and d_local and title):
                if debug:
                    print("[mvstv skip]", t_txt, d_local, title)
                continue
            local_midnight = datetime(d_local.year, d_local.month, d_local.day, 0, 0, 0)
            try:
                start_utc = parse_spanish_time(t_txt, local_midnight, tzname)
            except Exception:
                if debug:
                    print("[mvstv bad time]", t_txt)
                continue
            items.append({"date": d_local, "title": title, "start_utc": start_utc})

        # Order and build end times (next start on same day; else +60m)
        items.sort(key=lambda x: (x["date"], x["start_utc"]))
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
