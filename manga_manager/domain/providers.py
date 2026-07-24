from __future__ import annotations

from urllib.parse import urlparse


PROVIDER_ORIGINS = {
    "asura": "https://asurascans.com",
    "mangadex": "https://api.mangadex.org",
    "mangafire": "https://mangafire.to",
    "kingofshojo": "https://kingofshojo.com",
}


def provider_names() -> tuple[str, ...]:
    """Return the ordered backend provider registry used by dynamic UI slots."""
    return tuple(PROVIDER_ORIGINS)


KNOWN_SOURCES = provider_names()
SOURCE_PRIORITY = ("asura", "mangadex", "mangafire", "kingofshojo")


def source_for_origin(url: str) -> str | None:
    hostname = (urlparse(url).hostname or "").lower()
    for source, origin in PROVIDER_ORIGINS.items():
        if hostname == (urlparse(origin).hostname or "").lower():
            return source
    return None
