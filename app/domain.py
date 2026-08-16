from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher


SOURCE_PRIORITY = {
    "asura": 100,
    "mangadex": 80,
    "mangafire": 50,
    "kingofshojo": 10,
}


@dataclass(frozen=True)
class SeriesItem:
    source: str
    source_id: str
    title: str
    url: str
    aliases: tuple[str, ...] = ()
    description: str = ""
    cover_url: str = ""
    genres: tuple[str, ...] = ()
    popularity: float = 0
    external_ids: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ChapterItem:
    source: str
    source_series_id: str
    number: str
    title: str
    url: str
    published_at: datetime | None = None
    metadata: dict[str, object] = field(default_factory=dict)


def chapter_quality_rank(item: ChapterItem) -> int:
    """Return a provider-supplied quality rank without trusting arbitrary types."""
    value = item.metadata.get("quality_rank", 0)
    return int(value) if isinstance(value, (int, float)) else 0


def normalize_title(title: str) -> str:
    value = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    value = value.lower()
    value = re.sub(r"\([^)]*\)|\[[^]]*]", " ", value)
    value = re.sub(r"\b(the|a|an|manga|manhwa|manhua|webtoon|official)\b", " ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def normalize_chapter_number(value: str) -> str:
    match = re.search(r"(\d+(?:\.\d+)?)", value)
    if not match:
        return value.strip().lower()
    try:
        number = Decimal(match.group(1)).normalize()
    except InvalidOperation:
        return match.group(1)
    return format(number, "f")


def title_similarity(left: str, right: str) -> float:
    normalized_left = normalize_title(left)
    normalized_right = normalize_title(right)
    left_tokens = set(normalized_left.split())
    right_tokens = set(normalized_right.split())
    if not left_tokens or not right_tokens:
        return 0
    token_score = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    sequence_score = SequenceMatcher(None, normalized_left, normalized_right).ratio()
    return max(token_score, sequence_score)


def should_replace(current_source: str, candidate_source: str) -> bool:
    return SOURCE_PRIORITY.get(candidate_source, 0) > SOURCE_PRIORITY.get(current_source, 0)
