from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz, re, asyncio
from .base import Parser, Programme
from ..util.timeparse import parse_spanish_time, normalize_window

class MVSTVParser(Parser):
    domains = ["mvstv.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        """Parses https://mvstv.com/mvstv-programacion/ by rendering with Playwright,
        then extracting cards with time + title. The site is highly JS-driven, so
        we rely on page.content() and flexible heuristics.
        """
        results = []
        # Render
        if page:
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            html = await page.content()
        else:
            html = None

        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        local_tz = pytz.timezone(tzname)
        base_local = datetime.now(local_tz)

        # Heuristic: find any elements that look like schedule items, e.g. "10:00 AM Viernes, 29 de Ago · Caminos con Luz"
        text = soup.get_text("\n", strip=True)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        items = []
        for ln in lines:
            # Split at a bullet '·' or hyphen or colon
            if re.search(r"\b(\d{1,2}:\d{2}\s*(AM|PM)?)\b", ln, re.I):
                items.append(ln)

        programmes = []
        for ln in items:
            # Extract time and title by regex; assume duration ends at next item or +60min fallback
            m = re.search(r"(\d{1,2}:\d{2}\s*(?:AM|PM)?)\s+(.+)$", ln, re.I)
            if not m:
                continue
            start_txt = m.group(1)
            title_txt = m.group(2).strip("· -:|")

            try:
                start_utc = parse_spanish_time(start_txt, base_local, tzname)
            except Exception:
                continue
            # naive 60-minute bucket unless we can infer next start
            end_utc = start_utc + timedelta(minutes=60)
            programmes.append(Programme(title=title_txt, start=start_utc, end=end_utc))

        return normalize_window(programmes, hours_ahead)
