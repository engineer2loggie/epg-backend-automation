# scripts/parsers/mvstv.py
from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from urllib.parse import urlparse, parse_qs, unquote, unquote_plus
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
    m = DATE_RX.match((line or "").strip())
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
    return bool(re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", s))

def _parse_dates_param(dates_str: str, tzname: str):
    """
    Parse Google Calendar-style 'dates' values:
      20250831T153000Z/20250831T173000Z
      20250831T103000/20250831T123000  (local; assume tzname)
      (also tolerate missing seconds)
    Returns (start_utc, end_utc) or (None, None).
    """
    if not dates_str:
        return None, None
    s = unquote_plus(dates_str).strip()
    parts = [p.strip() for p in s.split("/") if p.strip()]
    if len(parts) != 2:
        return None, None

    def _parse_part(p: str):
        for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
            try:
                dt = datetime.strptime(p, fmt)
                if fmt.endswith("Z"):
                    return dt.replace(tzinfo=pytz.UTC)
                # local naive → localize
                return pytz.timezone(tzname).localize(dt).astimezone(pytz.UTC)
            except Exception:
                continue
        return None

    a = _parse_part(parts[0])
    b = _parse_part(parts[1])
    return a, b

class MVSTVParser(Parser):
    domains = ["mvstv.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        """
        Strategy:
          1) Render page (Playwright).
          2) Find all <a|button> whose text is 'AGENDAR'.
          3) For each, parse href/data-*:
               - If href has ?dates=... use that for start/end (exact).
               - Use title from 'text=' or data-title (fallback to DOM heading).
          4) If no calendar data, fallback to DOM (time line + date line + title).
          5) End = next start on same local date else +60m.
        """
        if not page:
            return []
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)

        # Extract structured info for AGENDAR controls
        agendar_info = await page.eval_on_selector_all(
            "a,button",
            """
els => els.filter(e => /\\bAGENDAR\\b/i.test(e.textContent || ''))
       .map(el => {
          const attrs = {};
          for (const a of el.attributes) attrs[a.name] = a.value;
          // attempt to find a reasonable card container
          let card = el.closest('article, .card, .program, .programa, .uk-card, .uk-card-body, .uk-grid, div');
          let titleEl = card ? (card.querySelector('h1,h2,h3,h4,strong,.title,.program-title') || null) : null;
          return {
            tag: el.tagName,
            href: attrs.href || '',
            attrs,
            titleFromDom: titleEl ? titleEl.textContent.trim() : '',
            cardText: card ? card.innerText : ''
          }
       })
""",
        )

        programmes = []

        # First pass: prefer calendar links for exact times
        for info in agendar_info:
            href = info.get("href") or ""
            attrs = info.get("attrs") or {}
            title = attrs.get("data-title") or info.get("titleFromDom") or ""

            start_utc = end_utc = None
            if href:
                try:
                    u = urlparse(href)
                    qs = parse_qs(u.query or "")
                    # Google Calendar template typically has 'text' & 'dates'
                    if not title and "text" in qs:
                        title = unquote_plus(qs["text"][0])
                    if "dates" in qs:
                        start_utc, end_utc = _parse_dates_param(qs["dates"][0], tzname)
                except Exception:
                    pass

            # Some implementations stash times in data-* attributes
            if (start_utc is None or end_utc is None) and attrs:
                ds = attrs.get("data-start") or attrs.get("data-from") or attrs.get("data-inicio")
                de = attrs.get("data-end") or attrs.get("data-to") or attrs.get("data-fin")
                # Accept ISO or “YYYYMMDDTHHMM” forms
                def _flexparse(s: str):
                    s = (s or "").strip()
                    if not s:
                        return None
                    try:
                        # Try ISO first
                        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = pytz.timezone(tzname).localize(dt)
                        return dt.astimezone(pytz.UTC)
                    except Exception:
                        pass
                    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
                        try:
                            dt = datetime.strptime(s, fmt)
                            if fmt.endswith("Z"):
                                return dt.replace(tzinfo=pytz.UTC)
                            return pytz.timezone(tzname).localize(dt).astimezone(pytz.UTC)
                        except Exception:
                            continue
                    return None
                if ds and de:
                    s = _flexparse(ds); e = _flexparse(de)
                    if s and e:
                        start_utc, end_utc = s, e

            if start_utc and end_utc and _looks_like_title(title):
                programmes.append(Programme(title=title.strip(), start=start_utc, end=end_utc))

        # If we found nothing via calendar links, fallback to DOM text (less precise)
        if not programmes:
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            # Use AGENDAR anchors as anchors for their cards, then derive time/date/title inside
            agenda_nodes = soup.find_all(string=re.compile(r"\bAGENDAR\b", re.I))
            cards = []
            for node in agenda_nodes:
                el = getattr(node, "parent", None)
                container = None
                for _ in range(8):
                    if not el:
                        break
                    block_text = el.get_text("\n", strip=True)
                    if re.search(r"\b\d{1,2}:\d{2}\s*(AM|PM)?\b", block_text, re.I) and DATE_RX.search(block_text):
                        container = el
                        break
                    el = getattr(el, "parent", None)
                if container:
                    cards.append(container)

            def _time_from_lines(lines):
                # merge ["10:30","AM"] => "10:30 AM"
                for i, ln in enumerate(lines):
                    m = re.search(r"\b(\d{1,2}:\d{2})\b", ln)
                    if m:
                        t = m.group(1)
                        mer = lines[i + 1] if i + 1 < len(lines) else ""
                        if ONLY_MERIDIEM_RX.match(mer or ""):
                            return f"{t} {mer}"
                        return t
                return None

            items = []
            for card in cards:
                lines = [ln.strip() for ln in card.get_text("\n", strip=True).splitlines() if ln.strip()]
                t_txt = _time_from_lines(lines)
                d_txt = next((ln for ln in lines if DATE_RX.match(ln)), None)
                title_dom = ""
                for h in card.select("h1,h2,h3,h4,strong,.title,.program-title"):
                    tt = h.get_text(" ", strip=True)
                    if _looks_like_title(tt):
                        title_dom = tt
                        break
                if not title_dom:
                    for ln in lines:
                        if DATE_RX.match(ln) or ONLY_MERIDIEM_RX.match(ln) or re.search(r"\b\d{1,2}:\d{2}\b", ln):
                            continue
                        if _looks_like_title(ln):
                            title_dom = ln
                            break
                if t_txt and d_txt and title_dom:
                    d_local = _parse_es_date(d_txt, tzname)
                    if d_local:
                        local_midnight = datetime(d_local.year, d_local.month, d_local.day, 0, 0, 0)
                        try:
                            s_utc = parse_spanish_time(t_txt, local_midnight, tzname)
                        except Exception:
                            s_utc = None
                        if s_utc:
                            items.append({"date": d_local, "title": title_dom, "start_utc": s_utc})

            # end = next start on same day; else +60m
            items.sort(key=lambda x: (x["date"], x["start_utc"]))
            for idx, it in enumerate(items):
                s = it["start_utc"]
                e = None
                for k in range(idx + 1, len(items)):
                    if items[k]["date"] == it["date"]:
                        e = items[k]["start_utc"]
                        break
                if e is None:
                    e = s + timedelta(minutes=60)
                programmes.append(Programme(title=it["title"], start=s, end=e))

        return normalize_window(programmes, hours_ahead)
