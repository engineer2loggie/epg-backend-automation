from __future__ import annotations
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as du
import pytz, re, os
from .base import Parser, Programme
from ..util.timeparse import normalize_window

TITLE_BAD_RX = re.compile(r"^(AM|PM|A\.M\.|P\.M\.|Close|Cerrar)$", re.I)

TITLE_SELECTORS = [
    ".program-title", ".programme-title", ".listing-title", ".title",
    ".program", ".programme", "h1", "h2", "h3", "h4", "a", "strong"
]
TIME_ATTRS = ["datetime","data-datetime","data-start","data-starttime","data-time","data-begin"]

def _parse_any_dt(s: str):
    if not s:
        return None
    s = s.strip()
    if s.isdigit():
        try:
            n = int(s)
            if n > 10_000_000_000:
                n //= 1000
            return datetime.fromtimestamp(n, tz=pytz.UTC)
        except Exception:
            pass
    try:
        dt = du.parse(s)
        return dt
    except Exception:
        return None

def _to_utc(dt, target_tz: str, mode: str):
    tz = pytz.timezone(target_tz)
    if dt.tzinfo is None:
        # assume guide’s local clock is target tz
        return tz.localize(dt).astimezone(pytz.UTC)
    if mode == "shift":
        # keep visible clock, reinterpret in target tz
        dt_local = tz.localize(datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, getattr(dt, "second", 0)))
        return dt_local.astimezone(pytz.UTC)
    # default 'convert'
    return dt.astimezone(pytz.UTC)

class OnTVTonightParser(Parser):
    domains = ["ontvtonight.com"]

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None):
        import httpx
        mode = os.getenv("ONTV_ALIGN_MODE", "convert").lower().strip()
        if mode not in ("convert","shift"): mode = "convert"

        r = httpx.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        programmes = []
        # Find every <time> that carries a starting timestamp; anchor the row by climbing a bit
        for t in soup.find_all("time"):
            raw = None
            for attr in TIME_ATTRS:
                if t.has_attr(attr):
                    raw = t.get(attr)
                    break
            if raw is None and t.get("datetime"):
                raw = t.get("datetime")
            if not raw:
                continue

            dt = _parse_any_dt(raw)
            if not dt:
                continue
            start_utc = _to_utc(dt, tzname, mode)

            # Row container: go up to a reasonable block (<li>, <tr>, or div with classes)
            row = t
            for _ in range(6):
                if not row.parent: break
                row = row.parent
                # Heuristic: stop when this block contains a title-looking element
                if any(row.select(sel) for sel in TITLE_SELECTORS):
                    break

            # Title within this row
            title = ""
            for sel in TITLE_SELECTORS:
                el = row.select_one(sel)
                if el:
                    txt = el.get_text(" ", strip=True)
                    if txt and not TITLE_BAD_RX.match(txt):
                        title = txt
                        break
            if not title:
                # fallback: longest non-empty string in row, excluding bad tokens
                best = ""
                for frag in row.stripped_strings:
                    frag = frag.strip()
                    if frag and not TITLE_BAD_RX.match(frag):
                        if len(frag) > len(best):
                            best = frag
                title = best

            if not title:
                continue

            programmes.append(Programme(title=title, start=start_utc, end=start_utc))  # temp end

        # Sort & compute end from next start (+60 min fallback)
        programmes.sort(key=lambda p: p.start)
        fixed = []
        for i, p in enumerate(programmes):
            s = p.start
            e = programmes[i+1].start if i+1 < len(programmes) else s + timedelta(minutes=60)
            fixed.append(Programme(title=p.title, start=s, end=e))

        return normalize_window(fixed, hours_ahead)
