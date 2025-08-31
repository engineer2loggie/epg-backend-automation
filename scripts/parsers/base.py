from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

@dataclass
class Programme:
    title: str
    start: datetime  # tz-aware UTC
    end: datetime    # tz-aware UTC

class Parser:
    """Interface for a site-specific parser."""
    domains: List[str] = []

    def matches(self, url: str) -> bool:
        from urllib.parse import urlparse
        netloc = urlparse(url).netloc.lower()
        return any(netloc.endswith(d) for d in self.domains)

    async def fetch_and_parse(self, url: str, *, tzname: str, hours_ahead: int, page=None) -> List[Programme]:
        raise NotImplementedError
