# Register all available parsers here
from .gatotv import GatoTVParser
from .ontvtonight import OnTVTonightParser  # you already have this file

ALL_PARSERS = [
    GatoTVParser(),
    OnTVTonightParser(),  # preferred for MVSTV via OnTVTonight
]
