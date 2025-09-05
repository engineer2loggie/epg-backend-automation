from __future__ import annotations
import re
import asyncio
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


@dataclass
class Programme:
    title: str
    start: datetime       # tz-aware
    end: datetime         # tz-aware
    category: Optional[str] = None
    description: Optional[str] = None


class LaochoParser:
    """
    Parser for https://laocho.tv/tv-programacion/
    - Interprets listed times as Europe/Madrid (site local).
    - Converts to tzname (e.g., 'America/New_York') for the returned Programme datetimes.
    - Returns items within a horizon of `hours_ahead` relative to *now in target tz*.
    """
    domains = ["laocho.tv"]
    _TIME_RE = re.compile(r"\b(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\b")

    def matches(self, url: str) -> bool:
        try:
            from urllib.parse import urlparse
            netloc = urlparse(url).netloc.lower()
            return any(netloc.endswith(d) for d in self.domains)
        except Exception:
            return False

    async def fetch_and_parse(
        self,
        url: str,
        tzname: str = "America/New_York",
        hours_ahead: int = 36,
        page: str | None = None,
    ) -> List[Programme]:
        # Keep async signature; do network + parse in a thread to avoid new deps
        html = await asyncio.to_thread(self._fetch_html, url)
        return self._parse_html(html, tzname=tzname, hours_ahead=hours_ahead)

    # --- internals ---

    def _fetch_html(self, url: str) -> str:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.text

    def _parse_html(self, html: str, tzname: str, hours_ahead: int) -> List[Programme]:
        soup = BeautifulSoup(html, "html.parser")

        tz_es = ZoneInfo("Europe/Madrid")
        tz_target = ZoneInfo(tzname)

        today_es = datetime.now(tz_es).date()
        # We’ll pair each TITLE node (h2/h3/h4) with the *next* node that contains a time range.
        title_nodes = [
            n for n in soup.find_all(["h2", "h3", "h4"])
            if n.get_text(strip=True)
        ]

        items: list[Programme] = []

        for title_node in title_nodes:
            title = title_node.get_text(" ", strip=True)
            # Find the next element containing HH:MM - HH:MM
            time_node = title_node.find_next(
                lambda t: t and t.name in ("h5", "h6", "p", "div", "span")
                and self._TIME_RE.search(t.get_text(" ", strip=True))
            )
            if not time_node:
                continue

            m = self._TIME_RE.search(time_node.get_text(" ", strip=True))
            if not m:
                continue

            sh, sm, eh, em = m.groups()

            start_es = datetime(
                today_es.year, today_es.month, today_es.day, int(sh), int(sm), tzinfo=tz_es
            )
            end_es = datetime(
                today_es.year, today_es.month, today_es.day, int(eh), int(em), tzinfo=tz_es
            )
            # Handle overnight wrap
            if end_es <= start_es:
                end_es += timedelta(days=1)

            # Optional short description: the next paragraph-ish node that isn't another timeblock
            desc = ""
            desc_node = time_node.find_next(
                lambda t: t and t.name in ("p", "div") and not self._TIME_RE.search(t.get_text(" ", strip=True))
            )
            if desc_node:
                desc = desc_node.get_text(" ", strip=True)

            # convert to target tz (US Eastern, etc.)
            start_target = start_es.astimezone(tz_target)
            end_target = end_es.astimezone(tz_target)

            items.append(Programme(
                title=title,
                start=start_target,
                end=end_target,
                category=None,
                description=desc or None
            ))

        # Filter to the requested horizon relative to now in target tz
        now_tz = datetime.now(tz_target)
        horizon_hi = now_tz + timedelta(hours=hours_ahead)
        items = [
            p for p in items
            if (p.end > now_tz and p.start < horizon_hi)
        ]

        # Ensure chronological order
        items.sort(key=lambda p: (p.start, p.title))
        return items
