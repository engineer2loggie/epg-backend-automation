# scripts/parsers/__init__.py
from __future__ import annotations

from .base import Parser  # for subclass discovery
from .gatotv import GatoTVParser

# --- Try to load On TV Tonight parser (robust import) ---
ONTV_PARSER = None
try:
    # Preferred explicit class
    from .ontvtonight import OnTVTonightParser as _OnTVTonightParser
    ONTV_PARSER = _OnTVTonightParser()
except Exception:
    # Fallback: auto-discover any subclass of Parser in the module
    try:
        from . import ontvtonight as _ott
        for _name in dir(_ott):
            _obj = getattr(_ott, _name)
            if isinstance(_obj, type) and issubclass(_obj, Parser) and _obj is not Parser:
                ONTV_PARSER = _obj()
                print(f"[parsers] Found OnTV parser class via auto-discovery: {_obj.__name__}")
                break
        if ONTV_PARSER is None:
            print("[parsers] OnTVTonight parser not found; "
                  "ensure it subclasses Parser and (ideally) is named OnTVTonightParser.")
    except Exception as e2:
        print("[parsers] Failed to import ontvtonight.py:", e2)

ALL_PARSERS = [GatoTVParser()]
if ONTV_PARSER:
    ALL_PARSERS.append(ONTV_PARSER)

# Debug: which parsers/domains loaded
try:
    loaded = ", ".join(type(p).__name__ for p in ALL_PARSERS)
    domains = [d for p in ALL_PARSERS for d in getattr(p, "domains", [])]
    print(f"[parsers] Loaded: {loaded}")
    print(f"[parsers] Domains: {domains}")
except Exception:
    pass
