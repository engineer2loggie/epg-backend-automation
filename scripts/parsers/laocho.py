from __future__ import annotations
import re
import asyncio
from dataclasses import dataclass
from typing import List, Optional, Iterable
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup, Tag

# ----------------------------
# Data model (unchanged API)
# ----------------------------
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
    - Converts to tzname (e.g., 'America/New_York') for returned Programme datetimes.
    - Returns items within a horizon of `hours_ahead` relative to *now in target tz*.
    """
    domains = ["laocho.tv"]

    # HH:MM - HH:MM
    _TIME_RE = re.compile(r"\b(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\b")
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )

    # Words we keep in caps even after normalization
    _PRESERVE_ALLCAPS = {"TV", "UHD", "HD", "4K", "3D", "TP", "PR", "MX", "ES", "USA"}
    # Common leading labels
    _LEADING_LABELS = ("SINOPSIS:", "SINOPSI:", "SYNOPSIS:", "DESCRIPCIÓN:", "DESCRIPCION:")

    # Category often appears as ALL-CAPS prefix like: "CINE: TÍTULO"
    _CATEGORY_MAP = {
        "CINE": "Cine",
        "SERIE": "Serie",
        "PELÍCULA": "Película",
        "PELICULA": "Película",
        "DEPORTE": "Deporte",
        "DEPORTES": "Deportes",
        "NOTICIAS": "Noticias",
        "INFORMATIVO": "Informativo",
        "REPORTAJE": "Reportaje",
        "ENTREVISTA": "Entrevista",
        "DOCUMENTAL": "Documental",
        "INFANTIL": "Infantil",
        "MAGAZINE": "Magazine",
    }

    # -------------- Public API --------------
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
        # network + parse in a thread to avoid blocking
        html = page or await asyncio.to_thread(self._fetch_html, url)
        items = self._parse_html(html, tzname=tzname, hours_ahead=hours_ahead)
        return items

    # -------------- Internals --------------
    def _fetch_html(self, url: str) -> str:
        r = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": self._UA, "Cache-Control": "no-cache"},
        )
        r.raise_for_status()
        return r.text

    @staticmethod
    def _neighbors_until(nodes: Iterable[Tag], stop_pred) -> List[Tag]:
        """Collect consecutive siblings until stop_pred(tag) is True."""
        out = []
        for n in nodes:
            if stop_pred(n):
                break
            out.append(n)
        return out

    def _parse_html(self, html: str, tzname: str, hours_ahead: int) -> List[Programme]:
        soup = BeautifulSoup(html, "html.parser")

        tz_es = ZoneInfo("Europe/Madrid")
        tz_target = ZoneInfo(tzname)

        today_es = datetime.now(tz_es).date()

        # Pair each TITLE node (h2/h3/h4) with the *next* node containing a time range.
        title_nodes = [
            n for n in soup.find_all(["h2", "h3", "h4"])
            if n.get_text(strip=True)
        ]

        items: list[Programme] = []

        for title_node in title_nodes:
            raw_title = title_node.get_text(" ", strip=True)

            # find the next sibling-ish element that contains a time range
            time_node = title_node.find_next(
                lambda t: t and isinstance(t, Tag)
                and t.name in ("h5", "h6", "p", "div", "span")
                and self._TIME_RE.search(t.get_text(" ", strip=True))
            )
            if not time_node:
                continue

            tm_txt = time_node.get_text(" ", strip=True)
            m = self._TIME_RE.search(tm_txt)
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

            # Build description from the run of following blocks up to the next title/time block.
            def _is_stop(t: Tag) -> bool:
                if not isinstance(t, Tag):
                    return False
                text = t.get_text(" ", strip=True)
                if not text:
                    return False
                # Stop if we see another title or a time block
                if t.name in ("h2", "h3", "h4"):
                    return True
                if self._TIME_RE.search(text):
                    return True
                return False

            desc_blocks = self._neighbors_until(time_node.find_all_next(recursive=False), _is_stop)
            # Fallback: also consider immediate siblings (DOMs vary)
            if not desc_blocks:
                desc_blocks = self._neighbors_until(time_node.next_siblings, _is_stop)

            desc_text = " ".join(
                b.get_text(" ", strip=True) for b in desc_blocks if isinstance(b, Tag)
            ).strip()

            # Normalize title/description (de-shout ALL-CAPS; preserve acronyms)
            title_norm, category = self._normalize_title_and_category(raw_title)
            desc_norm = self._normalize_sentence(desc_text) if desc_text else None

            # Convert to target tz
            start_target = start_es.astimezone(tz_target)
            end_target = end_es.astimezone(tz_target)

            items.append(Programme(
                title=title_norm,
                start=start_target,
                end=end_target,
                category=category,
                description=desc_norm
            ))

        # Filter to requested horizon (relative to now in target tz)
        now_tz = datetime.now(tz_target)
        horizon_hi = now_tz + timedelta(hours=hours_ahead)
        items = [p for p in items if (p.end > now_tz and p.start < horizon_hi)]

        # De-duplicate by (title, start)
        dedup = {}
        for p in items:
            k = (p.title, p.start)
            if k not in dedup:
                dedup[k] = p
        items = list(dedup.values())

        # Sort chronologically
        items.sort(key=lambda p: (p.start, p.title))
        return items

    # -------------- Normalization helpers --------------
    def _looks_all_caps(self, s: str) -> bool:
        return bool(s) and not any(ch.islower() for ch in s)

    def _normalize_sentence(self, s: str) -> str:
        """If ALL CAPS, make sentence case; keep known acronyms upper; strip leading labels."""
        if not s:
            return s

        txt = " ".join(s.split())
        # Remove leading 'SINOPSIS:'-like labels once
        up = txt.upper()
        for lbl in self._LEADING_LABELS:
            if up.startswith(lbl):
                txt = txt[len(lbl):].lstrip()
                break

        if not self._looks_all_caps(txt):
            return txt

        # Sentence case
        txt = txt.capitalize()

        # Restore acronyms inside punctuation
        out_words = []
        for w in txt.split():
            # keep punctuation like TP) intact
            prefix = ""
            suffix = ""
            core = w
            # strip leading punctuation
            while core and not core[0].isalnum():
                prefix += core[0]
                core = core[1:]
            # strip trailing punctuation
            while core and not core[-1].isalnum():
                suffix = core[-1] + suffix
                core = core[:-1]

            if core.upper() in self._PRESERVE_ALLCAPS:
                core = core.upper()

            out_words.append(prefix + core + suffix)

        return " ".join(out_words)

    def _normalize_title_and_category(self, raw_title: str) -> tuple[str, Optional[str]]:
        """
        Normalize ALL-CAPS title and extract leading CATEGORY: if present.
        Returns (normalized_title, category or None).
        """
        if not raw_title:
            return raw_title, None

        t = " ".join(raw_title.split())

        # Extract "CATEGORY: rest" if ALL-CAPS prefix and colon present
        category = None
        if ":" in t:
            left, right = t.split(":", 1)
            left_uc = left.strip().upper()
            if left_uc in self._CATEGORY_MAP and self._looks_all_caps(left):
                category = self._CATEGORY_MAP[left_uc]
                t = f"{category}: {right.strip()}"

        # If still ALL-CAPS, soften
        if self._looks_all_caps(t):
            # keep "Category: ..." format, normalize only the right side
            if category and ":" in t:
                cat, rest = t.split(":", 1)
                rest_norm = self._normalize_sentence(rest.strip())
                return f"{cat}: {rest_norm}", category
            else:
                return self._normalize_sentence(t), category

        return t, category
