from __future__ import annotations
import re
from datetime import datetime, timedelta
import pytz
from dateutil import parser as du

SPANISH_SECTIONS = {"Madrugada", "Mañana", "Tarde", "Noche"}

def parse_spanish_time(s: str, base_date_local: datetime, tzname: str):
    """Parse times like '6:30 AM', '22:15', '1:05 AM' relative to a base local date.
    Returns timezone-aware datetimes in UTC.
    """
    s = s.strip().replace('.', ':').replace('a. m.', 'AM').replace('p. m.', 'PM')
    # Normalize odd spacing
    s = re.sub(r'\s+', ' ', s)
    # dateutil handles AM/PM or 24h
    local_tz = pytz.timezone(tzname)
    naive = du.parse(s, default=base_date_local.replace(hour=0, minute=0, second=0, microsecond=0))
    if naive.tzinfo is None:
        naive = local_tz.localize(naive)
    else:
        naive = naive.astimezone(local_tz)
    return naive.astimezone(pytz.UTC)

def normalize_window(events, hours_ahead: int):
    """Ensure event end wraps to next day when end < start, filter to [now-6h, now+hours_ahead] UTC."""
    import pytz
    now = datetime.now(pytz.UTC)
    out = []
    for ev in events:
        start = ev.start
        end = ev.end
        if end <= start:
            end = end + timedelta(days=1)
        # Keep rows within desired window (slight backfill)
        if end >= now - timedelta(hours=6) and start <= now + timedelta(hours=hours_ahead):
            out.append(type(ev)(title=ev.title, start=start, end=end))
    # sort by channel/time
    out.sort(key=lambda x: (x.start, x.end, x.title))
    return out
