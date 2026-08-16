from __future__ import annotations

import re
from typing import Protocol
from urllib.parse import unquote, urlsplit


class ProviderIdentity(Protocol):
    source: str
    source_id: str
    url: str


def normalized_source_id(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", unquote(value).lower()).strip("-")
    return re.sub(r"-[0-9a-f]{8}$", "", normalized)


def canonical_source_url(value: str) -> str:
    parsed = urlsplit(value)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    path = unquote(parsed.path).rstrip("/").lower()
    return f"{host}{path}"


def provider_identities_equivalent(
    left: ProviderIdentity, right: ProviderIdentity
) -> bool:
    if left.source != right.source:
        return False
    left_id = normalized_source_id(left.source_id)
    right_id = normalized_source_id(right.source_id)
    if left_id and left_id == right_id:
        return True
    left_url = canonical_source_url(left.url)
    right_url = canonical_source_url(right.url)
    return bool(left_url and left_url == right_url)
