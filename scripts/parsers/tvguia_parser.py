from __future__ import annotations
import httpx
import pytz
from dataclasses import dataclass
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from typing import Optional
from urllib.parse import urlparse, urljoin

# This dataclass defines the structure for a programme entry.
@dataclass
class Programme:
    title: str
    start: datetime
    end: Optional[datetime]
    description: Optional[str] = None
    category: Optional[str] = None

# This is the base class your main script's `pick_parser` function needs.
class BaseParser:
    domains: list[str] = []

    @classmethod
    def matches(cls, url: str) -> bool:
        hostname = urlparse(url).hostname
        if not hostname:
            return False
        return any(hostname.lower().endswith(d) for d in cls.domains)

class TvGuiaParser(BaseParser):
    """Parses EPG data from tvguia.es."""
    domains = ["tvguia.es"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None) -> list[Programme]:
        # Helper function to filter results later
        def normalize_window(progs: list[Programme], hours: int) -> list[Programme]:
            now_utc = datetime.now(pytz.utc)
            window_end = now_utc + timedelta(hours=hours)
            return [p for p in progs if p.start < window_end]

        urls_to_scrape = [url, urljoin(url, "manana")]
        all_programmes = []

        async with httpx.AsyncClient() as client:
            for i, page_url in enumerate(urls_to_scrape):
                try:
                    resp = await client.get(page_url, timeout=20.0, follow_redirects=True)
                    resp.raise_for_status()
                    local_tz = pytz.timezone(tzname)
                    base_date = (datetime.now(local_tz) + timedelta(days=i)).date()
                    all_programmes.extend(self._parse_day(resp.text, base_date, tzname))
                except Exception as e:
                    print(f"Failed to fetch or parse {page_url}: {e}")
        
        self._infer_end_times(all_programmes)
        return normalize_window(all_programmes, hours_ahead)

    def _parse_day(self, html: str, base_date: date, tzname: str) -> list[Programme]:
        soup = BeautifulSoup(html, "lxml")
        local_tz = pytz.timezone(tzname)
        programmes = []
        for item in soup.find_all("article", class_="programacion-item"):
            try:
                time_str = item.find("span", class_="programacion-item-hour").text.strip()
                title = item.find("h3", class_="programacion-item-title").text.strip()
                category = item.find("span", class_="programacion-item-category")
                desc = item.find("div", class_="programacion-item-sinopsis")

                hour, minute = map(int, time_str.split(':'))
                start_naive = datetime.combine(base_date, datetime.min.time()).replace(hour=hour, minute=minute)
                start_aware = local_tz.localize(start_naive)

                programmes.append(Programme(
                    title=title,
                    start=start_aware.astimezone(pytz.utc),
                    end=None,
                    description=desc.text.strip() if desc else None,
                    category=category.text.strip() if category else None
                ))
            except (AttributeError, ValueError):
                continue
        return programmes

    def _infer_end_times(self, programmes: list[Programme]):
        programmes.sort(key=lambda p: p.start)
        for i, current in enumerate(programmes[:-1]):
            current.end = programmes[i+1].start
