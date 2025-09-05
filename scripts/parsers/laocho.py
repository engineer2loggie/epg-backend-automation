from __future__ import annotations
import re
import os
import time
import random
import asyncio
from dataclasses import dataclass
from typing import List, Optional, Iterable
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup, Tag


@dataclass
class Programme:
    title: str
    start: datetime
    end: datetime
    category: Optional[str] = None
    description: Optional[str] = None


class LaochoParser:
    domains = ["laocho.tv"]

    _TIME_RE = re.compile(r"\b(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\b")
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )

    _PRESERVE_ALLCAPS = {"TV", "UHD", "HD", "4K", "3D", "TP", "PR", "MX", "ES", "USA", "RTVE"}
    _LEADING_LABELS = ("SINOPSIS:", "SINOPSI:", "SYNOPSIS:", "DESCRIPCIÓN:", "DESCRIPCION:")
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

    # ---------- fetch with retries + optional Playwright ----------
    def _fetch_html(self, url: str) -> str:
        attempts = int(os.getenv("LAOCHO_HTTP_RETRIES", "4"))
        base = float(os.getenv("LAOCHO_HTTP_BACKOFF", "0.75"))
        force_requests = os.getenv("LAOCHO_FORCE_REQUESTS", "0") == "1"
        last_err = None
        for i in range(attempts):
            try:
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

    # ---------- parsing ----------
    def _parse_html(self, html: str, tzname: str, hours_ahead: int) -> List[Programme]:
        soup = BeautifulSoup(html, "html.parser")
        tz_es = ZoneInfo("Europe/Madrid")
        tz_target = ZoneInfo(tzname)
        today_es = datetime.now(tz_es).date()

        title_nodes = [n for n in soup.find_all(["h2", "h3", "h4"]) if n.get_text(strip=True)]
        items: list[Programme] = []

        for title_node in title_nodes:
            raw_title = title_node.get_text(" ", strip=True)

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
            if end_es <= start_es:
                end_es += timedelta(days=1)

            # Conservative description extraction
            desc_policy = os.getenv("LAOCHO_DESC_POLICY", "short").lower()
            desc_text = self._extract_description(time_node, policy=desc_policy)

            # Normalize
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

        # Window filter
        now_tz = datetime.now(tz_target)
        horizon_hi = now_tz + timedelta(hours=hours_ahead)
        items = [p for p in items if (p.end > now_tz and p.start < horizon_hi)]

        # De-dupe + sort
        seen = {}
        for p in items:
            key = (p.title, p.start)
            if key not in seen:
                seen[key] = p
        items = sorted(seen.values(), key=lambda p: (p.start, p.title))
        return items

    # ---------- description policy ----------
    def _extract_description(self, time_node: Tag, policy: str = "short") -> Optional[str]:
        """
        Conservative description builder:
        - 'none'  : no description
        - 'short' : first sentence only, max ~160 chars
        - 'full'  : previous multi-block behavior (up to first next title/time)
        """
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

        # Grab only the very next paragraph-like sibling
        next_text = None
        for sib in time_node.next_siblings:
            if not isinstance(sib, Tag):
                continue
            txt = sib.get_text(" ", strip=True)
            if not txt:
                continue
            if is_stop(sib):
                break
            # only accept simple <p>/<div>/<span>, avoid nested blocks / lists
            if sib.name in ("p", "div", "span"):
                next_text = txt
                break

        if not next_text:
            return None

        # Hard filters: skip promo/host bios (your example)
        up = next_text.upper()
        if up.startswith("PRESENTAN ") or "PRESENTAN " in up or up.startswith("CONDUCEN ") or up.startswith("CON "):
            return None

        # If policy is full, return trimmed multi-block text up to next program
        if policy == "full":
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
            return text if text else None

        # policy == short: first sentence only, length cap
        text = next_text
        # cut at first period followed by space/cap letter; fallback to hard cap
        m = re.search(r"\.(?=\s+[A-ZÁÉÍÓÚÜÑ])", text)
        if m:
            text = text[: m.end()].strip()
        if len(text) > 160:
            text = text[:157].rstrip() + "…"
        return text or None

    # ---------- normalization helpers ----------
    def _looks_all_caps(self, s: str) -> bool:
        return bool(s) and not any(ch.islower() for ch in s)

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
        txt = txt.capitalize()
        out_words = []
        for w in txt.split():
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
        if not raw_title:
            return raw_title, None
        t = " ".join(raw_title.split())
        category = None
        if ":" in t:
            left, right = t.split(":", 1)
            left_uc = left.strip().upper()
            if left_uc in self._CATEGORY_MAP and self._looks_all_caps(left):
                category = self._CATEGORY_MAP[left_uc]
                t = f"{category}: {right.strip()}"
        if self._looks_all_caps(t):
            if category and ":" in t:
                cat, rest = t.split(":", 1)
                rest_norm = self._normalize_sentence(rest.strip())
                return f"{cat}: {rest_norm}", category
            else:
                return self._normalize_sentence(t), category
        return t, category
