# scripts/parsers/laocho.py
from __future__ import annotations

import os
import re
import time
import random
import asyncio
from dataclasses import dataclass
from typing import List, Optional, Iterable
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup, Tag

# ============================================================
# Public data model (kept consistent with other parsers)
# ============================================================
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

    Behavior:
    - Times listed on page are in Europe/Madrid. We convert to the caller's tz (tzname).
    - Returns items within `hours_ahead` relative to *now in target tz*.
    - Title casing is normalized (ALL-CAPS → human-friendly), preserving acronyms.
    - Description capture is conservative and OFF by default (env LAOCHO_DESC_POLICY).
      * none  (default): no description is saved
      * short: first sentence (~160 chars)
      * full : multi-block text until next title/time block
    - Resilient fetching: retries with jitter and optional Playwright fallback.

    Env toggles (optional):
      LAOCHO_HTTP_RETRIES   (default "4")
      LAOCHO_HTTP_BACKOFF   (seconds, default "0.75")
      LAOCHO_FORCE_REQUESTS ("1" to disable Playwright fallback)
      USE_PLAYWRIGHT_FOR_LAOCHO ("1" to enable Playwright fallback)
      LAOCHO_DESC_POLICY    ("none" | "short" | "full", default "none")
    """
    domains = ["laocho.tv"]

    # --- HTTP ---
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )

    # --- Parsing helpers ---
    _TIME_RE = re.compile(r"\b(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\b")

    # Words/acronyms to preserve in uppercase after normalization
    _PRESERVE_ALLCAPS = {
        "TV", "UHD", "HD", "4K", "3D", "TP", "PR", "MX", "ES", "USA", "RTVE"
    }

    # Leading labels to strip from blurbs
    _LEADING_LABELS = ("SINOPSIS:", "SINOPSI:", "SYNOPSIS:", "DESCRIPCIÓN:", "DESCRIPCION:")

    # Common category prefixes that the site might use (ALL-CAPS)
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

    # Default to NO descriptions unless explicitly enabled
    os.environ.setdefault("LAOCHO_DESC_POLICY", "none")

    # -------------- Public API --------------
    def matches(self, url: str) -> bool:
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc.lower().endswith("laocho.tv")
        except Exception:
            return False

    async def fetch_and_parse(
        self,
        url: str,
        tzname: str = "America/New_York",
        hours_ahead: int = 36,
        page: str | None = None,
    ) -> List[Programme]:
        html = page or await asyncio.to_thread(self._fetch_html, url)
        return self._parse_html(html, tzname=tzname, hours_ahead=hours_ahead)

    # -------------- Networking (resilient) --------------
    def _fetch_html(self, url: str) -> str:
        """
        Resilient fetch with exponential backoff + jitter and cache-busting.
        Falls back to Playwright if enabled and requests fail.
        """
        attempts = int(os.getenv("LAOCHO_HTTP_RETRIES", "4"))
        base = float(os.getenv("LAOCHO_HTTP_BACKOFF", "0.75"))
        force_requests = os.getenv("LAOCHO_FORCE_REQUESTS", "0") == "1"
        last_err = None

        for i in range(attempts):
            try:
                # Cache-busting param on retries to avoid intermediary caches/WAF
                bust = "" if i == 0 else (f"?cb={int(time.time()*1000)}")
                target = url + bust
                r = requests.get(
                    target,
                    timeout=30,
                    headers={
                        "User-Agent": self._UA,
                        "Cache-Control": "no-cache",
                        "Accept-Language": "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
                    },
                )
                r.raise_for_status()
                return r.text
            except Exception as e:
                last_err = e
                time.sleep(base * (2 ** i) + random.uniform(0, 0.25))

        if not force_requests and os.getenv("USE_PLAYWRIGHT_FOR_LAOCHO", "0") == "1":
            html = self._fetch_with_playwright(url)
            if html:
                return html

        raise last_err

    def _fetch_with_playwright(self, url: str) -> str | None:
        """Optional headless Chromium fallback (requires Playwright to be installed)."""
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(user_agent=self._UA, java_script_enabled=True)
                page = ctx.new_page()
                page.set_default_timeout(30000)
                page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
                html = page.content()
                browser.close()
                return html
        except Exception:
            return None

    # -------------- Parsing --------------
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

        # Title blocks are typically h2/h3/h4; each followed by a time range somewhere nearby.
        title_nodes = [n for n in soup.find_all(["h2", "h3", "h4"]) if n.get_text(strip=True)]
        items: list[Programme] = []

        for title_node in title_nodes:
            raw_title = title_node.get_text(" ", strip=True)

            # Find the next element that contains <HH:MM - HH:MM>
            time_node = title_node.find_next(
                lambda t: t and isinstance(t, Tag)
                and t.name in ("h5", "h6", "p", "div", "span")
                and self._TIME_RE.search(t.get_text(" ", strip=True))
            )
            if not time_node:
                continue

            m = self._TIME_RE.search(time_node.get_text(" ", strip=True))
            if not m:
                continue

            sh, sm, eh, em = m.groups()
            start_es = datetime(today_es.year, today_es.month, today_es.day, int(sh), int(sm), tzinfo=tz_es)
            end_es = datetime(today_es.year, today_es.month, today_es.day, int(eh), int(em), tzinfo=tz_es)
            if end_es <= start_es:  # overnight wrap
                end_es += timedelta(days=1)

            # Description (conservative) — governed by env policy (default 'none')
            desc_policy = os.getenv("LAOCHO_DESC_POLICY", "none").lower()
            desc_text = self._extract_description(time_node, policy=desc_policy)

            # Normalize title and optional description
            title_norm, category = self._normalize_title_and_category(raw_title)
            desc_norm = self._normalize_sentence(desc_text) if (desc_text and desc_policy != "none") else None

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

        # Window filter (relative to now in target tz)
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

    # -------------- Description policy --------------
    def _extract_description(self, time_node: Tag, policy: str = "none") -> Optional[str]:
        """
        Conservative description builder:
          - 'none'  : no description (default)
          - 'short' : first sentence only, ~160 chars, avoids promo/host bios
          - 'full'  : concatenates siblings until next title/time block
        """
        policy = (policy or "none").lower()
        if policy == "none":
            return None

        def is_stop(t: Tag) -> bool:
            if not isinstance(t, Tag):
                return False
            text = t.get_text(" ", strip=True)
            if not text:
                return False
            if t.name in ("h2", "h3", "h4"):
                return True
            if self._TIME_RE.search(text):
                return True
            return False

        # Take at most the *immediate* simple paragraph-like sibling
        next_text = None
        for sib in time_node.next_siblings:
            if not isinstance(sib, Tag):
                continue
            txt = sib.get_text(" ", strip=True)
            if not txt:
                continue
            if is_stop(sib):
                break
            if sib.name in ("p", "div", "span"):  # conservative
                next_text = txt
                break

        if not next_text:
            return None

        # Skip obvious promos/host bios (e.g., "Presentan ...")
        up = next_text.upper()
        if up.startswith("PRESENTAN ") or " PRESENTAN " in up or up.startswith("CONDUCEN ") or up.startswith("CON "):
            return None

        if policy == "full":
            # Concatenate until next title/time block
            blocks: List[str] = []
            for sib in time_node.next_siblings:
                if not isinstance(sib, Tag):
                    continue
                if is_stop(sib):
                    break
                txt = sib.get_text(" ", strip=True)
                if txt:
                    blocks.append(txt)
            text = " ".join(blocks).strip()
            return text or None

        # policy == "short"
        text = next_text
        # First sentence heuristic (period followed by space + capital)
        m = re.search(r"\.(?=\s+[A-ZÁÉÍÓÚÜÑ])", text)
        if m:
            text = text[: m.end()].strip()
        # Length cap (~160 chars)
        if len(text) > 160:
            text = text[:157].rstrip() + "…"
        return text or None

    # -------------- Normalization helpers --------------
    def _looks_all_caps(self, s: str) -> bool:
        # ALL-CAPS if there are letters and none are lowercase (handles accents)
        return bool(s) and any(ch.isalpha() for ch in s) and not any(ch.islower() for ch in s)

    def _normalize_sentence(self, s: str) -> str:
        if not s:
            return s
        txt = " ".join(s.split())
        up = txt.upper()
        for lbl in self._LEADING_LABELS:
            if up.startswith(lbl):
                txt = txt[len(lbl):].lstrip()
                break
        if not self._looks_all_caps(txt):
            return txt

        lowered = txt.lower()
        if lowered:
            lowered = lowered[0].upper() + lowered[1:]

        # Restore acronyms in uppercase while preserving punctuation
        out_words = []
        for w in lowered.split():
            prefix = ""
            suffix = ""
            core = w
            while core and not core[0].isalnum():
                prefix += core[0]
                core = core[1:]
            while core and not core[-1].isalnum():
                suffix = core[-1] + suffix
                core = core[:-1]
            if core.upper() in self._PRESERVE_ALLCAPS:
                core = core.upper()
            out_words.append(prefix + core + suffix)
        return " ".join(out_words)

    def _normalize_title_and_category(self, raw_title: str) -> tuple[str, Optional[str]]:
        """
        Normalize titles like:
          'SERIE: LOS ÁNGELES...'  or  'Serie: LOS ÁNGELES...'
        → 'Serie: Los ángeles...'
        Extract a category when the left part matches known keys (case-insensitive).
        """
        if not raw_title:
            return raw_title, None

        t = " ".join(raw_title.split())
        if ":" in t:
            left, right = t.split(":", 1)
            left_clean = left.strip()
            right_clean = right.strip()

            # Map known categories case-insensitively
            left_key = left_clean.upper()
            category = self._CATEGORY_MAP.get(left_key)

            # De-shout the right side if it's ALL-CAPS
            if self._looks_all_caps(right_clean):
                right_norm = self._normalize_sentence(right_clean)
            else:
                right_norm = right_clean

            if category:
                return f"{category}: {right_norm}", category
            else:
                # Left isn't a known category; still normalize the right side if needed
                return f"{left_clean}: {right_norm}", None

        # No colon: soften whole title if ALL-CAPS
        if self._looks_all_caps(t):
            return self._normalize_sentence(t), None

        return t, None
