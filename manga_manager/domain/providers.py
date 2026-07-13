from __future__ import annotations

from urllib.parse import urlparse


PROVIDER_ORIGINS = {
    "asura": "https://asurascans.com",
    "kingofshojo": "https://kingofshojo.com",
    "mangafire": "https://mangafire.to",
}
KNOWN_SOURCES = frozenset(PROVIDER_ORIGINS)
SOURCE_PRIORITY = ("asura", "mangafire", "kingofshojo")


def source_for_origin(url: str) -> str | None:
    hostname = (urlparse(url).hostname or "").lower()
    for source, origin in PROVIDER_ORIGINS.items():
        if hostname == (urlparse(origin).hostname or "").lower():
            return source
    return None
