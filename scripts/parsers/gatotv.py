from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz, re, asyncio
from .base import Parser, Programme
from ..util.timeparse import parse_spanish_time, normalize_window

class GatoTVParser(Parser):
    domains = ["gatotv.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        """Parses GatoTV channel pages like https://www.gatotv.com/canal/5_mexico
        Strategy:
          - Use Playwright page (if provided) to render and get HTML (works for both static/JS).
          - Find the 'Horarios de Programación' table and read rows as (start,end,title).
          - Also try to follow 'next day' link (›) once to reach 36h horizon.
        """
        results = []
        # Use provided page for requests (ensures cookies/headers); otherwise fetch with httpx
        html, next_href = await self._load_day(url, page=page)
        results += self._parse_day(html, tzname=tzname)

        # Try to grab next-day link for spillover
        if next_href:
            if page:
                await page.goto(next_href, wait_until="domcontentloaded")
                html2 = await page.content()
            else:
                import httpx
                r = httpx.get(next_href, timeout=30)
                r.raise_for_status()
                html2 = r.text
            results += self._parse_day(html2, tzname=tzname)

        return normalize_window(results, hours_ahead)

    async def _load_day(self, url: str, page=None):
        if page:
            await page.goto(url, wait_until="domcontentloaded")
            html = await page.content()
        else:
            import httpx
            r = httpx.get(url, timeout=30)
            r.raise_for_status()
            html = r.text
        # try to extract next-day link (">" / "›")
        soup = BeautifulSoup(html, "lxml")
        next_href = None
        # look for a link containing › or 'Lunes', 'Mañana', 'Horarios para hoy' navigation
        for a in soup.find_all("a"):
            if a.get_text(strip=True) in {"›", "+", "Siguiente", "Lunes 1 ›"} or "›" in a.get_text():
                href = a.get("href")
                if href and href.startswith("http"):
                    next_href = href
                elif href and href.startswith("/"):
                    from urllib.parse import urljoin
                    next_href = urljoin(url, href)
        return html, next_href

    def _parse_day(self, html: str, *, tzname: str):
        soup = BeautifulSoup(html, "lxml")
        # Base date: try to detect a 'Horarios para hoy' context; fallback to local today
        local_tz = pytz.timezone(tzname)
        base_local = datetime.now(local_tz)

        rows = []
        # Heuristic: rows appear after a header 'Horarios de Programación' and contain three columns
        # with start/end/time and program name.
        table_candidates = []
        for tag in soup.find_all(["table","div","section"]):
            text = tag.get_text(" ", strip=True)
            if "Horarios de Programación" in text or "Hora Inicio" in text:
                table_candidates.append(tag)

        # If not found, fallback to scanning for time-time-title triplets
        if not table_candidates:
            return []

        # Choose the largest candidate
        node = max(table_candidates, key=lambda t: len(t.get_text()))

        text = node.get_text("\n", strip=True)
        # Build a simple state machine to collect (start,end,title)
        # Pattern: lines sometimes come grouped: start, end, title
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        triples = []
        i = 0
        while i < len(lines)-2:
            s, e, t = lines[i], lines[i+1], lines[i+2]
            # A valid time is like 6:30 AM or 22:15
            if self._looks_like_time(s) and self._looks_like_time(e) and not self._looks_like_time(t):
                triples.append((s, e, t))
                i += 3
            else:
                i += 1

        out = []
        for s,e,t in triples:
            try:
                start_utc = parse_spanish_time(s, base_local, tzname)
                end_utc   = parse_spanish_time(e, base_local, tzname)
                out.append(Programme(title=t, start=start_utc, end=end_utc))
            except Exception:
                continue
        return out

    def _looks_like_time(self, s: str) -> bool:
        s = s.strip().upper()
        return bool(re.match(r"^(\d{1,2}[:\. ]\d{2})\s*(AM|PM)?$", s)) or bool(re.match(r"^\d{1,2}[:\.]\d{2}$", s))
